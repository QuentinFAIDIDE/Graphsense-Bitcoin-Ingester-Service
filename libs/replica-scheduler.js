var redis = require("redis");
var cq = require("concurrent-queue");
var randomstring = require("randomstring");

var TIMEOUT_SIMPLE_REQUEST = 7000;
var LOCK_TIMEOUT = 20000;
var TIMEOUT_START_MASTER = 10000;


// the desired probability for two repilca to health check in the same second together
var HEALTH_CHECK_RACE_PROBA_SECOND = 0.02;
// we will dynamically scale our random numbers and jitters to have 
// control over the probabilities of race conditions given number of replicas
// the statistical analysis js code has a function for that
var { findDistributedProtocolFreq } = require("./statistical-analysis.js");

class ReplicaScheduler {
    /*
    Class responsible for making replicas talk to each other
    and decide who gets to take master role when nobody is
  */

    constructor(redisHost, redisPort, symbol, logMessage, logErrors, debug, masterStartFunc) {
        // save "this" pointer since nodejs loose it when entering callbacks
        let parentExec = this;

        // create the redis client
        this._redisClient = redis.createClient({ port: redisPort, host: redisHost });
        this._subClient = redis.createClient({ port: redisPort, host: redisHost });

        // save important constructor variables
        this._currency = symbol;
        this._logMessage = logMessage;
        this._logErrors = logErrors;
        this._debug = debug;

        // this should be dynamically adjusted pretty soon
        // but let's use some defaults
        this._healthCheckRateSec = 0.1;
        this._nreplicas = 5;

        this._debug("Starting the replica scheduler...");

        this._identifier = String(Date.now())+randomstring.generate(8);

        // channel identifier construction
        this._redisSchedulerChannel = symbol + "::" + "replica-scheduling";

        // callback on schedule redis channel subscription
        this._subClient.on("subscribe", (channel, count)=>{
            console.log("Replica sheduler subscribed to redis channel: "+channel);
        });

        // callback on message received through redis subscriptions
        this._subClient.on("message", (channel, message)=>{
            // if it's from the right channel
            if(channel==parentExec._redisSchedulerChannel) {
                // parse the message using our private function
                parentExec._parseChannelMessage(message);
            }
        });

        this._startService = masterStartFunc;

        // queue to avoid local concurrent healthChecks
        this._queue = cq().limit({ concurrency: 1, maxSize: 5}).process(function (task) {
            return new Promise((resolve,reject)=>{
                if(task=="healthCheck") {
                    // if we are not electing succesor due to shutdown already, or not in anothe health check
                    if(parentExec._electingSuccessor==false && parentExec._healthCheckRunning==false) {
                        // run the health check
                        parentExec._execHealthCheck().then(()=>{
                            // as soon as it's done, save the date and resolve
                            parentExec._lastHealthCheckDate = Date.now();
                            resolve();
                        }).catch(reject);
                    } else {
                        resolve();
                        return;
                    }
                } else {
                    // in case of unrecognised task pushed to queue (shouldn't happen)
                    parentExec._logMessage("Replica Scheduler received unrecognised queue task:"+task);
                    reject("Replica Scheduler received unrecognised queue task:"+task);
                }
            });
        });

        this._election;
        this._healthCheckResponses = [];
        this._healthCheckMasterCount = 0;
        this._lastHealthCheckDate = 0;
        this._electingSuccessor=false;
        this._healthCheckRunning = false;
        this._shutdownElectionID = randomstring.generate(12);
        this._electionCandidates = [];
        this._running = false;

        this._stopped = false;
    }

    // PRIVATE METHODS
    /* act on received messages on the redis channel for replicas scheduling */
    /* message is formatted as so: "[TIMESTAMP]::[IDENTIFIER_EMITTER]::[ACTION]::[PARAMETER1,PARAMETER2,etc..]*/
    _parseChannelMessage(msg) {
        try {
            // parsing the message parameters
            let messageContent = String(msg).split("::");
            let parameters = messageContent[4].split(",");
            // if the message is a service signaling it's shutting down and need the name of those awaiting
            if(messageContent[3]=="SHUTDOWN_SIG" && parameters[0]=="request_candidates") {
                this._debug("Received message:"+msg);
                // ignore the message if the emitter is us
                if(messageContent[2]==this._identifier) {
                    return;
                }
                // save the identifier of the request to give our name
                let electionID = parameters[1];
                // send our name back to the shutdownsignal emitter (reminder: sendMessage gives the id)
                this._sendMessage("SHUTDOWN_SIG", "response_candidates,"+electionID);
            // if the signal is a reply for a SHUTDOWNSIG's election's procedure
            } else if(messageContent[3]=="SHUTDOWN_SIG" && parameters[0]=="response_candidates") {
                this._debug("Received message:"+msg);
                // if the election request is not from us, ignore
                if(this._electingSuccessor==false || this._shutdownElectionID!=parameters[1]) return;
                // else, if the election id matched the one we got in memory, push response to a list
                this._electionCandidates.push({
                    id: messageContent[2],
                    timestamp: Number(messageContent[1])
                });
            // if the message is a an election signal after a shutdown election
            } else if(messageContent[3]=="ELECTED_REPLICA") {
                this._debug("Received message:"+msg);
                // if the elected replica is us
                if(parameters[0]==this._identifier) {
                    // start the callback to run the service
                    this._runAsMaster();
                    return;
                }
            // if someone is running a health check
            } else if(messageContent[3]=="HEALTHCHECK" && parameters[0]=="request") {
                // if we are currently running the service
                if(this._running==true) {
                    // reply with a message signaling we are master
                    this._sendMessage("HEALTHCHECK", "response,"+this._identifier+","+parameters[1]+",master");
                } else {
                    // same for workers
                    this._sendMessage("HEALTHCHECK", "response,"+this._identifier+","+parameters[1]+",worker");
                }
            // if someone is replying to a healthcheck
            } else if(messageContent[3]=="HEALTHCHECK" && parameters[0]=="response") {
                // if it's our healthcheck
                if(this._healthCheckRunning==true && this._healthCheckID==parameters[2]) {
                    // log the response
                    this._healthCheckResponses.push({
                        "id": parameters[1],
                        "timestamp": messageContent[1],
                        "is_master": (parameters[3]=="master")
                    });
                    // increment number of master if it is
                    if(parameters[3]=="master")
                        this._healthCheckMasterCount++;
                }
            // if a health check decided to kill one of many replicas simultaneously running the master
            } else if(messageContent[3]=="RACE_KILL") {
                // if we are the target
                if(parameters[0]==this._identifer) {
                    // kill the process
                    this._logMessage("Replica Scheduler: another replica running a health check asked us to stop running the master service");
                    process.exit(0);
                }
            } else if(messageContent[3]=="REPLICA_COUNT") {
                // save the number of replicas
                this._nreplicas = Number(parameters[0]);
                this._healthCheckRateSec = findDistributedProtocolFreq(HEALTH_CHECK_RACE_PROBA_SECOND, this._nreplicas);
                this._debug("Health check was computed to have a probability of "+this._healthCheckRateSec+" to happen at every second.");
            }
        } catch (err) {
            this._logErrors(
                "ReplicaScheduler: Error while parsing the redis channel message:" +
                err);
        }
    }

    _sendMessage(action, parameters) {
        this._redisClient.publish(this._redisSchedulerChannel, this._currency+"::"+Date.now()+"::"+this._identifier+"::"+action+"::"+parameters);
    }

    // private method called to start playing the master role
    _runAsMaster() {

        // clear all incoming healthCheck
        // clearTimeout(this._nextHealthCheckTimeout);
        // we maintain health check for master, we just prevent they don't seppuku if duplicate masters

        // if the callback is not defined, display error and exit
        if(typeof this._startService === "undefined") {
            this._logMessage("Replica Scheduler tried to start an uninitialized replica, closing it.");
            process.exit(1);
        }
        // start
        this._running = true;
        // leave some time before effectively starting the master 
        // role for health check to detect hypothetical race conditions
        setTimeout(()=>{this._startService();}, TIMEOUT_START_MASTER);
    }

    // to call in order to start the health checkings and job scheduling
    startScheduler() {
        this._debug("Starting working as Scheduler (connect to redis pub/sub and start intervals).");
        // check if the service has not already been cleared
        if(this._stopped==true) {
            this._logErrors("Can't restart cleared replica scheduler. Create a new one");
            return;
        }

        // subscribe to the appropirate redis channel for redis replica scheduling
        this._subClient.subscribe(this._redisSchedulerChannel);
        // set the pointer to this for use inside callbacks
        let parentExec = this;
        // each second run health check with probability computed dynamically
        this._healthCheckInterval = setInterval(()=>{
            if(Math.random()<parentExec._healthCheckRateSec) {
                parentExec._queue("healthCheck").catch(parentExec._debug);
            }
        }, 1000);
    }

    // stop listening to redis, stop health check
    stopScheduler() {
        this._subClient.unsubscribe(this._redisSchedulerChannel);
        if(typeof this._healthCheckInterval != "undefined") {
            clearInterval(this._healthCheckInterval);
        }
        // close redis clients
        this._subClient.quit();
        this._redisClient.quit();
        this._stopped = true;
    }

    // return true if we are currently running as master
    isRunningAsMaster() {
        return this._running;
    }


    // check if we have a master monitoring other microservices
    _execHealthCheck() {
        let parentExec = this;
        return new Promise((resolve,reject)=>{
            // if health check is already running do nothing (shouldn't happen due to queue)
            if(parentExec._healthCheckRunning == true) {
                resolve();
                return;
            }
            // if last health check was less than one second ago, don't do it again
            if(Math.abs(parentExec._lastHealthCheckDate-Date.now())<1000) {
                resolve();
                return;
            }
            // generate a health check id
            parentExec._healthCheckID = randomstring.generate(8);
            // set lock::health_check to our worker id, health check id, with a timestamp (if not existing with SETNX)
            parentExec._redisClient.setnx(""+parentExec._currency+"::"+"health-check-lock", ""+Date.now()+"::"+parentExec._healthCheckID, (errSNX, resSNX)=>{
                // error handling
                if(errSNX) {
                    reject(errSNX);
                    return;
                }
                // if setnx worked
                if(resSNX==1) {
                    // wait a tiny timeout
                    setTimeout(()=>{
                        // get the value of the lock to check it's indeed ours
                        parentExec._redisClient.get(""+parentExec._currency+"::"+"health-check-lock", (errGET,resGET)=>{
                            // error handling
                            if(errGET) {
                                reject(errGET);
                                return;
                            }                   
                            // if nothing was found (race condition if some other healthcheck terminating deleted our setnx)
                            // we can just ignore it, and ignore this healthcheck
                            if(resGET==null) {
                                reject(new Error("Replica Scheduler: lock key disapeared after getting it."));
                                return;
                            }         
                            // if it is ours
                            if(resGET.split("::")[1]==parentExec._healthCheckID) {
                                // set the bool to say health check is running
                                parentExec._healthCheckRunning = true;
                                // clear the array to log the responses and nb of masters
                                parentExec._healthCheckResponses = [];
                                parentExec._healthCheckMasterCount = 0;
                                // send a message to the channel
                                parentExec._sendMessage("HEALTHCHECK", "request,"+parentExec._healthCheckID);
                                // wait a bit
                                setTimeout(()=>{
                                    // write the number of candidates
                                    parentExec._sendMessage("REPLICA_COUNT", ""+parentExec._healthCheckResponses.length);
                                    // if more than two master responses arrived
                                    if(parentExec._healthCheckMasterCount>=2) {
                                        // number of replicas to kill
                                        let nkill = parentExec._healthCheckResponses-1;
                                        // for each response after the first one
                                        for(let i=1; i<parentExec._healthCheckResponses.length;i++) {
                                            // break out of loop if nothing left to kill
                                            if(nkill==0)break;
                                            // send a KILL request if it's a master
                                            if(parentExec._healthCheckResponses[i].is_master==true) {
                                                nkill--;
                                                parentExec._sendMessage("RACE_KILL", parentExec._healthCheckResponses[i].id);
                                            }
                                        }
                                        // now, unset the bool to know if health check is running
                                        parentExec._healthCheckRunning=false;
                                        // liberate the lock
                                        parentExec._redisClient.del(""+parentExec._currency+"::"+"health-check-lock");
                                        // then resolve
                                        resolve();
                                        return;
                                    // or if no one is actually running anything
                                    } else if(parentExec._healthCheckMasterCount==0) {
                                        // start the service
                                        parentExec._startService();
                                        parentExec._running = true;
                                        // now, unset the bool to say health check is running
                                        parentExec._healthCheckRunning=false;
                                        // liberate the lock
                                        parentExec._redisClient.del(""+parentExec._currency+"::"+"health-check-lock");
                                        // then resolve
                                        resolve();
                                        return;
                                    } else {
                                        // liberate the lock
                                        parentExec._redisClient.del(""+parentExec._currency+"::"+"health-check-lock");
                                        // nothing special to report about
                                        parentExec._healthCheckRunning=false;
                                        resolve();
                                        return;
                                    }
                                },TIMEOUT_SIMPLE_REQUEST);
                            // if the lock is not ours, do nothing
                            } else {
                                parentExec._debug("race condition with another replica for the lock");
                                resolve();
                                return;
                            }
                        });
                    }, TIMEOUT_SIMPLE_REQUEST);
                // if setnx failed
                } else {
                    // get the key value
                    parentExec._redisClient.get(""+parentExec._currency+"::"+"health-check-lock", (errGET,resGET)=>{
                        // health check
                        if(errGET) {
                            reject(errGET);
                            return;
                        }
                        // if the lock does exists
                        if(resGET!=null) {
                            // if the timestamp within the lock is not too old (>LOCK_TIMEOUT)
                            if(Number(resGET.split("::")[0])<(Date.now()-(LOCK_TIMEOUT))) {
                                // remove it
                                parentExec._debug("deleting timed out lock");
                                parentExec._redisClient.del(""+parentExec._currency+"::"+"health-check-lock");
                                resolve();
                                return;
                            }
                            resolve();
                            return;
                        } else {
                            // nothing to do the value does not exists
                            resolve();
                            return;
                        }
                    });
                }
            });
        });
    }
}

module.exports = {ReplicaScheduler};
