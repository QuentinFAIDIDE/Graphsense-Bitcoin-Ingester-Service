var redis = require("redis");

var DELAY_CHECK_JOB = 5000;

const fs = require("fs");
const net = require("net");
const { exec } = require("child_process");
const {CassandraWriter} = require("./cassandra-writer.js");
const randomstring = require("randomstring");

let MAX_JOBS_PER_REPLICA = 1;
// the best value to avoid crashing a single bitcoin node
let MAX_FILL_CONCURRENCY = 9;
let MAX_FILL_CONCURRENCY_PER_NODE = 9;

if(typeof process.env.MAX_FILL_CONCURRENCY_PER_NODE != "undefined") {
    MAX_FILL_CONCURRENCY_PER_NODE=process.env.MAX_FILL_CONCURRENCY_PER_NODE;
    if(Number.isNaN(Number(MAX_FILL_CONCURRENCY_PER_NODE))==true 
    || Number(MAX_FILL_CONCURRENCY_PER_NODE)<1) {
        this._logErrors("MAX_FILL_CONCURRENCY_PER_NODE invalid");
        process.exit(1);
    }
}

// detect if redis password setting is used
var USE_REDIS_PASSWORD = !(typeof process.env.REDIS_PASSWORD == "undefined");
var REDIS_PASSWORD = process.env.REDIS_PASSWORD;

class WorkerRole {
    /*
    Class that implements worker role that all replica have
    (pull work from list, fetch blocks and transactions, etc...)
  */

    constructor(redisHost, redisPort, symbol, logMessage, logErrors, debug) {
        // save "this" pointer since nodejs loose it when entering callbacks
        let parentExec = this;

        // create the redis client
        if(USE_REDIS_PASSWORD==false) {
            this._redisClient = redis.createClient({ port: redisPort, host: redisHost });
            this._subClient = redis.createClient({ port: redisPort, host: redisHost });
        } else {
            this._redisClient = redis.createClient({ port: redisPort, host: redisHost, password: REDIS_PASSWORD });
            this._subClient = redis.createClient({ port: redisPort, host: redisHost, password: REDIS_PASSWORD });
        }

        
        // save important constructor variables
        this._currency = symbol;
        this._logMessage = logMessage;
        this._logErrors = logErrors;
        this._debug = debug;

        this._debug("Starting the worker...");

        this._identifier = String(Date.now())+randomstring.generate(8);

        // channel identifier construction
        this._redisSchedulerChannel = symbol + "::" + "job-scheduling";

        // initialize casandra writer object
        this._cassandraWriter = new CassandraWriter(redisHost, redisPort, symbol, logMessage, logErrors, debug);

        // callback on schedule redis channel subscription
        this._subClient.on("subscribe", (channel, count)=>{
            console.log("Worker manager subscribed to redis channel: "+channel);
        });

        // callback on message received through redis subscriptions
        this._subClient.on("message", (channel, message)=>{
            // if it's from the right channel
            if(channel==parentExec._redisSchedulerChannel) {
                // parse the message using our private function
                parentExec._parseChannelMessage(message);
            }
        });

        this._jobCount = 0;

        // schedules updates of dynamic concurrency setting depending on number of bitcoin nodes used
        this._updateMaxFillConcurrencyInterval = setInterval(()=>{this._updateMaxFillConcurrency();}, 30000);
        this._updateMaxFillConcurrency();
    }

    // PRIVATE METHODS
    /* act on received messages on the redis channel for replicas scheduling */
    /* message is formatted as so: "[CURRENCY]::[TIMESTAMP]::[IDENTIFIER_EMITTER]::[ACTION]::[PARAMETER1,PARAMETER2,etc..]*/
    _parseChannelMessage(msg) {
        try {
            // parsing the message parameters
            let messageContent = String(msg).split("::");
            let parameters = messageContent[4].split(",");
            // if someone is running a CALLROLL
            if(messageContent[3]=="CALLROLL" && parameters[0]=="request") {
                // reply with a message signaling we are alive
                this._sendMessage("CALLROLL", "response,"+this._identifier+","+messageContent[2]);
            // if someone is replying to a healthcheck
            } else if(messageContent[3]=="KILL") {
                // if we are the target
                if(parameters[0]==this._identifier) {
                    // kill the process
                    this._logMessage("Worker: A master running a CALLROLL asked us to stop running service");
                    process.exit(0);
                }
            }
        } catch (err) {
            this._logErrors(
                "WorkerRole: Error while parsing the redis channel message:" +
                err);
        }
    }

    _sendMessage(action, parameters) {
        this._redisClient.publish(this._redisSchedulerChannel, this._currency+"::"+Date.now()+"::"+this._identifier+"::"+action+"::"+parameters);
    }

    // to call in order to start the health checkings and job scheduling
    startWorker() {
        // check if the service has not already been cleared
        if(this._stopped==true) {
            this._logErrors("Can't restart cleared worker. Create a new one");
            return;
        }
        // subscribe to the appropirate redis channel for redis replica scheduling
        this._subClient.subscribe(this._redisSchedulerChannel);
        // at this point, we will start looping over task to do
        this._checkJobInterval = setInterval(()=>{this._checkAvailabilityForWork();}, DELAY_CHECK_JOB);
    }

    _checkAvailabilityForWork() {
        // if we have room left for a job
        if(this._jobCount<MAX_JOBS_PER_REPLICA) {
            // we start polling for one
            this._jobCount++;
            this._pollForJob(this._jobCount-1).then((jobname)=>{
                if(jobname!="no job found" && jobname)
                    this._debug("Worker finished job:"+jobname);
                this._jobCount--;
            }).catch((err)=>{
                this._logErrors("Worker error for job:"+err);
                this._jobCount--;
            });
        }
    }

    _pollForJob(i) {
        
        return new Promise((resolve,reject)=>{

            // do a pull on the job list 
            this._redisClient.lpop(""+this._currency.toUpperCase()+"::jobs::todo", (errBLP, resBLP)=>{
                // error handling
                if(errBLP) {
                    reject("REDIS ERROR (worker._pollForJob: "+errBLP);
                    return;
                }

                if(resBLP==null) {
                    resolve("no job found");
                    return;
                }

                // encapsulate in a try catch to make sure we indeed remove jobcount
                try {
                    // "todo" jobs are made of 3 "::"-separated fields:
                    // KEYSPACE_IDENTIFIER::JOBTYPE::PARAM,PARAM2,...

                    // the "inprogess" stack has 4 "::"-separated fields:
                    // KEYSPACE::JOBTYPE::ASSIGNEE_ID::param1,param2,...
                    // same for "finished" stack

                    this._debug("received job: "+resBLP);

                    // stream updates to cli clients
                    this._redisClient.publish(""+this._currency.toUpperCase()+"::picked", resBLP);

                    // parse job message
                    let jobFields = resBLP.split("::");
                    let parameters = jobFields[2].split(",");

                    if(jobFields[1]=="ENRICH_BLOCK_RANGE") {
                        // this job looks like:
                        // KEYSPACE_IDENTIFIER::ENRICH_BLOCK_RANGE::FIRST_BLOCK,LAST_BLOCK
                        this._startEnrichJob(jobFields[0], parameters[0], parameters[1]).then(()=>{
                            resolve(resBLP);
                        }).catch((err)=>{
                            this._logErrors(err);
                            this._moveJobToErrorStack(resBLP).then(()=>{
                                reject(err);
                            }).catch(reject);
                        });
                    } else if (jobFields[1]=="FILL_BLOCK_RANGE") {
                        // this job looks like:
                        // KEYSPACE_IDENTIFIER::FILL_BLOCK_RANGE::FIRST_BLOCK,LAST_BLOCK
                        this._getActiveJobsForTask("FILL_BLOCK_RANGE").then((fill_jobs_processing)=>{
                            // if we have more than MAX_FILL_CONCURRENCY workers on the FILL_BLOCK_RANGE job
                            if(fill_jobs_processing>=MAX_FILL_CONCURRENCY) {
                                // if we push to the beginning we risk a race condition with an enrichment job push
                                // if we push to the end, we will flip the fill jobs and delay next enrich jobs generation a bit
                                // the best deal is to push to the beginning, it will create a one-filling-job skewness while flipping jobs order might create 10
                                this._redisClient.lpush(""+this._currency.toUpperCase()+"::jobs::todo", resBLP, (errLPR, resLPR)=>{
                                    if(errLPR) {
                                        reject(errLPR);
                                    } else {
                                        resolve();
                                    }
                                });
                            } else {
                                this._startBlockRangeFillJob(jobFields[0], parameters[0], parameters[1]).then(()=>{
                                    resolve(resBLP);
                                // in case of problem, move the job to the error stack
                                }).catch((err)=>{
                                    this._logErrors(err);
                                    this._moveJobToErrorStack(resBLP).then(()=>{
                                        reject(err);
                                    }).catch(reject);
                                });
                            }
                        });
                    }
                } catch(err) {
                    reject(err);
                }
            });
        });
    }

    _getActiveJobsForTask(task) {
        return new Promise((resolve, reject)=>{
            // get the list of jobs in the doing stack
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::jobs::doing", 0, 20, (errLR, resLR)=>{
                // error handling
                if(errLR) {
                    reject(errLR);
                    return;
                }
                // if no job in doing, return zero
                if(resLR==null || (Array.isArray(resLR)==true && resLR.length==0)) {
                    resolve(0);
                    return;
                }
                // if no error, count jobs that match this task
                let nb = 0;
                for(let i=0;i<resLR.length;i++) {
                    if(resLR[i].indexOf(task)!=-1)nb++;
                }
                resolve(nb);
                return;
            });
        });
    }

    _streamBlocksAndTransactions(minblock, maxblock, block_callback, transaction_callback) {
        return new Promise((resolve,reject)=>{
            // generate a random identifier
            let taskid = randomstring.generate(8);
            // blocks and transaction tasks filenames
            let bfilename = taskid+"-blocks";
            let tfilename = taskid+"-transactions";
            // we use that boolean to avoid rejecting and then resolveing when pipes closed
            var rejectedWithError = false;

            // this callback is to be called when everything has been processed
            let everythingFinishedCallback = ()=>{
                // resolve
                if(rejectedWithError == false)
                    resolve();
                // clear the pipes files
                exec("rm pipes/"+bfilename+".json pipes/"+tfilename+".json", (errRM, stdoRM, stdeRM)=>{
                    if(errRM) {
                        this._logErrors("ERROR(worker._streamBlocksAndTransactions): Unable to delete the pipe files:"+errRM);
                    }
                });
            };

            // callback to monitor closed pipes
            let nclosed = 0;
            let pipeclosed = ()=>{
                nclosed++;
                // if all pipes are finished
                if(nclosed==2) {
                    everythingFinishedCallback();
                }
            };

            // create the two pipe files
            exec("mkfifo pipes/"+bfilename+".json pipes/"+tfilename+".json", (err,stdo,stde)=>{
                // error handling
                if(err) {
                    rejectedWithError = true;
                    reject("Error:"+err);
                    return;
                }
                // open the blocks pipe
                fs.open("pipes/"+bfilename+".json", fs.constants.O_RDONLY | fs.constants.O_NONBLOCK, (err, fd)=>{
                    // error handling
                    if(err) {
                        rejectedWithError = true;
                        reject("error while opening block pipe file:"+err);
                        return;
                    }
                    
                    // create the socket from the file
                    const pipe = new net.Socket({fd});
                    // redirect to callback
                    pipe.on("data", block_callback);
                    pipe.on("end", pipeclosed);
                    pipe.on("error", (err)=>{this._logErrors("block pipe err:"+err);});
                    

                    // open the transaction pipe
                    fs.open("pipes/"+tfilename+".json", fs.constants.O_RDONLY | fs.constants.O_NONBLOCK, (err2, fd2)=>{
                        // error handling
                        if(err2) {
                            rejectedWithError = true;
                            reject("error while opening transaction pipe file:"+err2);
                            return;
                        }
                        // create the socket from the file
                        var pipe2 = new net.Socket({fd:fd2});

                        // redirect to callback
                        pipe2.on("data", transaction_callback);
                        pipe2.on("end", pipeclosed);
                        pipe2.on("error", (err)=>{this._logErrors("transaction pipe err:"+err);});

                        // now it's time to launch our command the write to the pipes !
                        // but first we must choose on of the longest unused bitcoin client
                        this._redisClient.lmove(this._currency+"::node-clients",
                            this._currency+"::node-clients", 
                            "RIGHT", "LEFT", (errLMOV, resLMOV)=>{
                            // error checking
                                if(errLMOV) {
                                    rejectedWithError = true;
                                    reject("REDIS ERROR (worker._streamBlocksAndTransactions): "+errLMOV);
                                    pipe.end();
                                    pipe2.end();
                                    return;
                                }

                                // if no node contact info was found
                                if(resLMOV==null || resLMOV=="") {
                                    rejectedWithError = true;
                                    reject("Empty node host list (worker._streamBlocksAndTransactions)");
                                    pipe.end();
                                    pipe2.end();
                                    return;
                                }

                                // if everything is alright, we launch the command
                                let command = "bitcoinetl export_blocks_and_transactions --start-block "+minblock+
                                " --end-block "+maxblock+" -w 1 -p http://"+process.env.CRYPTO_CLIENTS_LOGIN+
                                ":"+process.env.CRYPTO_CLIENTS_PWD+"@"+resLMOV+" --chain bitcoin --blocks-output pipes/"+
                                bfilename+".json --transactions-output pipes/"+tfilename+".json ";

                                this._debug("Calling command: "+command);
                                // spawn the child
                                var childETLProcess = exec(command);
                                // let's just redirect ETL's stderr in case we might miss important logs
                                childETLProcess.stderr.on("data", (logtxt)=>{
                                    this._debug("ETL client stderr:"+logtxt);
                                    // we need to parse for errors in case the exit code is not 1 (it happens sometimes)
                                    if(logtxt.indexOf("Internal Server Error")!=-1 || logtxt.indexOf("Traceback")!=-1) {
                                        rejectedWithError = true;
                                        reject("ETL Process rejected with Internal Server Error: "+logtxt);
                                    }
                                });
                                childETLProcess.stdout.on("data", (logtxt)=>{this._debug("ETL client stdout:"+logtxt);});
                                // on close, make sure pipes are closed
                                childETLProcess.on("close", (code)=>{
                                    // now, if there was an error code, reject, or if all good, wait for end of pipes
                                    if(code!=0) {
                                        rejectedWithError = true;
                                        reject("ETL Process rejected with code:"+code);
                                    }
                                });
                            });
                    });
                });
            });
        });
    }

    // stop listening to redis, stop health check
    stopWorker() {
        // NOTE: no need to drop the work in the doing piles
        // the lack of ping response will make master reset jobs
        this._subClient.unsubscribe(this._redisSchedulerChannel);
        // close redis clients
        this._subClient.quit();
        this._redisClient.quit();
        this._stopped = true;
    }

    // block range importing job (for block and transacts!)
    _startBlockRangeFillJob(keyspace, minblock, maxblock) {
        return new Promise((resolve,reject)=>{
            // generate elem to push to in progress stack
            let jobname = ""+keyspace+"::FILL_BLOCK_RANGE::"+this._identifier+"::"+minblock+","+maxblock;
            // wrap up call to reject and move job to error stack simultaneously
            let rejectAndMoveToErrorStack = (err)=>{
                this._cassandraWriter.clearFillingJobIfExists(jobname);
                this._logErrors(err);
                this._moveJobToErrorStack(jobname).then(()=>{reject(err);}).catch(reject);
            };
            // push the job to the list of work in progress
            this._redisClient.lpush(""+this._currency+"::jobs::doing", jobname, (errLP, resLPL)=>{
                if(errLP) {
                    reject("REDIS ERROR (worker._startBlockRangeFillJob): Unable to push job to doing list: "+errLP);
                    return;
                }

                // now we stream blocks and transactions to the cassandra writing function
                this._cassandraWriter.prepareForKeyspace(keyspace).then(()=>{
                    // register job error watcher
                    try {
                        // prevent race conditions
                        let alreadyCalledBack = false;
                        // this also register the callback for when all the write have been confirmed
                        this._cassandraWriter.registerFillingJob(jobname, ()=>{
                            this._cassandraWriter.getFillingJobStatus(jobname).then((err)=>{
                                // prevent race conditions that may occur on error handling and waiting for cassandra I/O simultaneously
                                if(alreadyCalledBack==false) {
                                    alreadyCalledBack = true;
                                } else {
                                    return;
                                }
                                if(err==null) {
                                    this._moveJobFromDoingToDone(jobname).then(()=>{
                                        resolve();
                                        // now let the master know this range is finished
                                        // (it will test all finished ranges to generate tx enrichment (input inference) jobs)
                                        this._redisClient.rpush(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspace, 
                                            ""+minblock+","+maxblock, (errLP, resLP)=>{
                                                if(errLP) {
                                                    this._logErrors("(REDIS FATAL ERROR) Unable to push to redis filled block stack: "+errLP);
                                                    this._markKeyspaceAsBroken(keyspace);
                                                }
                                            });
                                    }).catch(reject);
                                } else {
                                    rejectAndMoveToErrorStack(err);
                                }
                            }).catch((err)=>{rejectAndMoveToErrorStack(err);});
                        });
                    } catch(err) {
                        rejectAndMoveToErrorStack(err);
                        return;
                    }
                    // start streaming transactions
                    this._streamBlocksAndTransactions(minblock, maxblock, (data)=>{
                        this._cassandraWriter.parseBlock(keyspace,jobname,data);
                    }, (data)=>{
                        this._cassandraWriter.parseTransaction(keyspace, jobname, data);
                    }).then(()=>{
                        this._debug("Finished streaming blocks and txs to writer class...");
                    }).catch(rejectAndMoveToErrorStack);
                }).catch(rejectAndMoveToErrorStack);
            });
        });
    }

    // test job
    _startEnrichJob(keyspace, minblock, maxblock) {
        return new Promise((resolve,reject)=>{
            // generate elem to push to in progress stack
            let jobname = ""+keyspace+"::ENRICH_BLOCK_RANGE::"+this._identifier+"::"+minblock+","+maxblock;
            // wrap up call to reject and move job to error stack simultaneously
            let rejectAndMoveToErrorStack = (err)=>{
                this._logErrors(err);
                this._moveJobToErrorStack(jobname).then(()=>{reject(err);}).catch(reject);
            };
            // push the job to the list of work in progress
            this._redisClient.lpush(""+this._currency+"::jobs::doing", jobname, (errLP, resLPL)=>{
                if(errLP) {
                    reject("REDIS ERROR (worker._startEnrichJob): Unable to push job to doing list: "+errLP);
                    return;
                }

                let returned = false;

                // prepare the keyspace if it's not
                this._cassandraWriter.prepareForKeyspace(keyspace).then(()=>{
                    // prepare for the job and allocate necessary objects
                    this._cassandraWriter.registerEnrichingJob(jobname, (err)=>{
                        if(returned==false) {
                            returned = true;
                        } else {
                            return;
                        }
                        if(err==null) {
                            // save the job range as done for master to increment keyspace statistics
                            // all good, we can move the job in the done stack
                            this._moveJobFromDoingToDone(jobname).then(()=>{
                                this._redisClient.rpush(""+this._currency.toUpperCase()+"::"+keyspace+"::enriched-ranges", ""+minblock+","+maxblock,
                                    (errLP2, resLP2)=>{
                                        if(errLP2) {
                                        // redis rpush error
                                            this._logErrors("Failed to write enriched range to redis for master to update statistics");
                                            this._markKeyspaceAsBroken(keyspace);
                                            return;
                                        } else {
                                            resolve();
                                        }
                                    });
                            }).catch(reject);
                        } else {
                            rejectAndMoveToErrorStack(err);
                            return;
                        }
                    });
                }).catch(rejectAndMoveToErrorStack);
            });
        });
    }

    _markKeyspaceAsBroken(keyspace) {
        // if the redis instance is temporarly down and it made the tasks fail, waiting a bit might increase success rate
        setTimeout(()=>{
            this._redisClient.hset(this._currency.toUpperCase()+"::monitored::"+keyspace, "broken", "true", (errSet)=>{
                if(errSet) {
                    this._logErrors("Fatal redis error:"+ errSet);
                    process.exit(1);
                }
                // notify users (all errors are also streamed to cli clients)
                this._logErrors("Keyspace "+keyspace+" was marked as broken.");
            });
        },5000);
    }

    _moveJobFromDoingToDone(jobname) {
        return new Promise((resolve, reject)=>{
            // remove the job from the doing list
            this._redisClient.lrem(""+this._currency+"::jobs::doing", 1, jobname, (errLREM, resLREM)=>{
                if(errLREM) {
                    reject("Lost a job we couldn't remove from todo list: "+ jobname);
                    return;
                }

                // check if we indeed removed something
                // if not throw an error
                if(resLREM==0) {
                    reject("Job missing from doing list:"+jobname);
                    return;
                }

                // push it to done list
                this._redisClient.lpush(""+this._currency+"::jobs::done", jobname, (errLP, resLP)=>{
                    if(errLP) {
                        reject("Lost a job we couldn't push to done list:"+jobname);
                        return;
                    }

                    // resolve
                    resolve();
                });
            });
        });
    }

    _moveJobToErrorStack(jobname) {
        return new Promise((resolve, reject)=>{
            // remove the job from the doing list
            this._redisClient.lrem(""+this._currency+"::jobs::doing", 1, jobname, (errLREM, resLREM)=>{
                if(errLREM) {
                    reject("Lost a job we couldn't remove from todo list: "+ jobname);
                    return;
                }

                // check if we indeed removed something
                // if not throw an error
                if(resLREM==0) {
                    // let's simplify things and change it to resolve()
                    // the job is probably already pushed to error list
                    resolve();
                    return;
                }

                // push it to error list
                this._redisClient.lpush(""+this._currency+"::jobs::errors", jobname, (errLP, resLP)=>{
                    if(errLP) {
                        reject("Lost a job we couldn't push to error list:"+jobname);
                        return;
                    }

                    // stream error updates to cli clients
                    this._redisClient.publish(""+this._currency.toUpperCase()+"::errors", "job failed to execute: "+jobname);

                    // resolve
                    resolve();
                });
            });
        });
    }

    _updateMaxFillConcurrency() {
        // get the number of bitcoin nodes available
        this._redisClient.llen(this._currency+"::node-clients", (errLL, resLL)=>{
            if(errLL) {
                this._logErrors("Error while updating the fill job concurrency:"+errLL);
                this._logErrors(errLL);
                return;
            }

            // if nothing was found, set to as if there was only one node 
            if(resLL==null || Number.isNaN(Number(resLL))==true || resLL==0) {
                MAX_FILL_CONCURRENCY = MAX_FILL_CONCURRENCY_PER_NODE;
            // if we have number of node, do the math
            } else {
                MAX_FILL_CONCURRENCY = MAX_FILL_CONCURRENCY_PER_NODE * Number(resLL);
            }
        });
    }
}



module.exports = {WorkerRole};
