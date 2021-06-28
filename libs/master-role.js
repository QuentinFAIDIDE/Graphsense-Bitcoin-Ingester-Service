var redis = require("redis");
var randomstring = require("randomstring");
var request =require("request");
var { CassandraWriter } = require("./cassandra-writer.js");

var MASTER_JOBCHECK_INTERVAL = 60000;
var CALLROLL_DELAY = 3000;
var BLOCK_BATCH_SIZE = 50;
var MIN_RATE_DATE = "2010-10-17";

var MAX_TODO_STACK_LEN = 10;

class MasterRole {
    /*
    Class that implements master role
    (push work to list, watch keyspaces, workers, etc...)
  */

    constructor(redisHost, redisPort, symbol, logMessage, logErrors, debug) {

        // create the redis client
        this._redisClient = redis.createClient({ port: redisPort, host: redisHost });
        this._subClient = redis.createClient({ port: redisPort, host: redisHost });

        // save important constructor variables
        this._currency = symbol;
        this._logMessage = logMessage;
        this._logErrors = logErrors;
        this._debug = debug;

        this._debug("Starting the master...");

        this._identifier = String(Date.now())+randomstring.generate(8);

        // channel identifier construction
        this._redisJobChannel = symbol + "::" + "job-scheduling";

        // callback on schedule redis channel subscription
        this._subClient.on("subscribe", (channel, count)=>{
            console.log("Master manager subscribed to redis channel: "+channel);
        });

        // callback on message received through redis subscriptions
        this._subClient.on("message", (channel, message)=>{
            // if it's from the right channel
            if(channel==this._redisJobChannel) {
                // parse the message using our private function
                this._parseChannelMessage(message);
            }
        });

        this._workersIds = [];

        this._cassandraWriter = new CassandraWriter(redisHost,redisPort, symbol, logMessage, logErrors, debug);
    }

    // PRIVATE METHODS
    /* act on received messages on the redis channel for replicas scheduling */
    /* message is formatted as so: "[TIMESTAMP]::[IDENTIFIER_EMITTER]::[ACTION]::[PARAMETER1,PARAMETER2,etc..]*/
    _parseChannelMessage(msg) {
        try {
            // parsing the message parameters
            let messageContent = String(msg).split("::");
            let parameters = messageContent[4].split(",");
            // if someone is responding to a call roll
            if(messageContent[3]=="CALLROLL" && parameters[0]=="response") {
                this._workersIds.push(parameters[1]);
            }
        } catch (err) {
            this._logErrors(
                "masterRole: Error while parsing the redis channel message:" +
                err);
        }
    }

    _sendMessage(action, parameters) {
        this._redisClient.publish(this._redisJobChannel, this._currency+"::"+Date.now()+"::"+this._identifier+"::"+action+"::"+parameters);
    }

    // to call in order to start the service
    startMaster() {
        this._debug("Master role assigned, starting working as Master");
        // check if the service has not already been cleared
        if(this._stopped==true) {
            this._logErrors("Can't restart cleared replica scheduler. Create a new one");
            return;
        }
        // subscribe to the appropirate redis channel for redis replica scheduling
        this._subClient.subscribe( ""+this._currency + "::" + "job-scheduling");
        // send the list of nodes to redis list
        var CRYPTO_CLIENTS = process.env.CRYPTO_CLIENTS;
        var crypto_clients_endpoints = CRYPTO_CLIENTS.split(",");
        this._writeCryptoClients(crypto_clients_endpoints);
        
        // now it's time to start the master routine check intervals
        this._jobCheckInterval = setInterval(()=>{
            this._runJobCheck();
        }, MASTER_JOBCHECK_INTERVAL);
    }

    _runJobCheck() {
        this._debug("Master run liveliness probe for workers");
        // are all jobs in the "doing" column have responding workers ?
        this._checkActiveJobLiveliness().then(()=>{
            // get last block available
            this._getLastAvailableBlock().then((maxheight)=>{
                this._debug("Max height in nodes:"+maxheight);

                // error handling
                if(maxheight==-1) {
                    // error was already logged
                    // simply returning
                    return;
                }
                
                this._redisClient.llen(""+this._currency.toUpperCase()+"::jobs::todo", (errLLE, todoStackLength)=>{
                    if(errLLE) {
                        reject(errLLE);
                        return;
                    }
                    // get all keyspaces JSONs objects on watch list
                    this._getMonitoredKeyspaces().then((keyspaces)=>{
                        this._debug("Keyspaces monitored found: "+JSON.stringify(keyspaces));
                        // create a mult to bulk redis calls
                        let mult = this._redisClient.multi();
                        // for each one, if it has been marked as to update continously
                        for(let i=0; i<keyspaces.length;i++) {
                            if(keyspaces[i].feedFrom!=-1) {
                                // if last block availabl minus confirm delay is bigger than last keyspace block
                                if(Number(keyspaces[i].lastWrittenBlock) < Number(maxheight) - Number(keyspaces[i].delay)) {
                                    // push a job for the new block
                                    let range = [Number(keyspaces[i].lastWrittenBlock)+1 ,Number(maxheight) - Number(keyspaces[i].delay)];
                                    
                                    let jobname = keyspaces[i].name+"::FILL_BLOCK_RANGE::"+range[0]+","+range[1];

                                    let tasksPending = todoStackLength;
                            
                                    // split job into subtask of blck size
                                    let furthest_block_sent = Number(range[0]-1);
                                    let end = Number(range[1]);
                                    let split_size = BLOCK_BATCH_SIZE;
                                    while ((furthest_block_sent + split_size) <= end && tasksPending<MAX_TODO_STACK_LEN) {
                                        tasksPending++;
                                        // sending job for subrange
                                        jobname = keyspaces[i].name+"::FILL_BLOCK_RANGE::"+
                                            (furthest_block_sent+1)+","+String(furthest_block_sent + split_size);
                                        // push the job in the redis bulk call objects
                                        mult.rpush(this._currency.toUpperCase()+"::jobs::todo", jobname);
                                        // increment to next batch
                                        furthest_block_sent += split_size;
                                    }

                                    // if there is a remainder, send it as well
                                    if (furthest_block_sent < end && tasksPending<MAX_TODO_STACK_LEN) {
                                        // sending job for subrange
                                        jobname = keyspaces[i].name+"::FILL_BLOCK_RANGE::"+
                                            (furthest_block_sent+1)+","+(end);
                                        furthest_block_sent = end;
                                        // push the job in the redis bulk call objects
                                        mult.rpush(this._currency.toUpperCase()+"::jobs::todo", jobname);
                                    }

                                    // update the last written block for the keyspace
                                    mult.hset(this._currency.toUpperCase()+"::monitored::"+keyspaces[i].name, "lastWrittenBlock", furthest_block_sent);
                                }
                                // if rates were not set
                                if(keyspaces[i].hasOwnProperty("lastRatesWritten")==false) {
                                    // get yesterdays date in YYYY-MM-DD format
                                    let yestedayTxt = this._getYesterdayDateTxt();
                                    // make the mult update the date before entering promise-waiting a state
                                    mult.hset(this._currency.toUpperCase()+"::monitored::"+keyspaces[i].name, "lastRatesWritten", yestedayTxt);
                                    // call the function to update all rates in this namespace
                                    this._updateRatesForKeyspaceForRange(keyspaces[i].name, null, yestedayTxt);
                                // else, check if some might be missing
                                } else {
                                    // get today's date minus one day
                                    let yestedayTxt = this._getYesterdayDateTxt();
                                    // get last rates written dates
                                    let lastRatesWritten = keyspaces[i].lastRatesWritten;
                                    // if some days are missing
                                    if(yestedayTxt!=lastRatesWritten) {
                                        // make the mult update the last date written
                                        //   before entering a promise waiting state
                                        mult.hset(this._currency.toUpperCase()+"::monitored::"+keyspaces[i].name, "lastRatesWritten", yestedayTxt);
                                        // call the function to update rates in this namespace
                                        this._updateRatesForKeyspaceForRange(keyspaces[i].name, lastRatesWritten, yestedayTxt);
                                    }
                                }
                            }
                        }
                        // execute the db calls
                        mult.exec((errMult, resMult)=>{
                            if(errMult) {
                                this._logErrors(errMult);
                            } else {
                                this._debug("Succesfully updated monitored keyspaces and dead workers");
                            }
                        });

                    }).catch(this._logErrors);
                });
            }).catch(this._logErrors);
        }).catch(this._logErrors);
    }

    // simply returns yesterday's date in a YYYY-MM-DD format
    _getYesterdayDateTxt() {
        let yesterday = new Date(Date.now()-(60000*60*24));
        return this._dayAsText(yesterday);
    }

    // get the day of the date ass YYYY-MM-DD
    _dayAsText(date) {
        let month = date.getMonth()+1;
        let monthTxt;
        if(String(month).length==1) {
            monthTxt = "0"+month;
        } else {
            monthTxt = String(month);
        }
        let day = date.getDate();
        let dayTxt;
        if(String(dayTxt).length==1) {
            dayTxt = "0"+day;
        } else {
            dayTxt = String(day);
        }
        return ""+date.getFullYear()+"-"+monthTxt+"-"+dayTxt;
    }


    _updateRatesForKeyspaceForRange(keyspace, from, to) {
        if(from==null) {
            from = MIN_RATE_DATE;
        }
        this._debug("Updating rates from "+from+" to "+to);
        // prepare the keyspace
        this._cassandraWriter.prepareForKeyspace(keyspace).then(()=>{
            // once its ready, get the btc rates
            this._getRatesFromCoindesk("EUR" ,from, to).then((eur_rates)=>{
                this._getRatesFromCoindesk("USD" ,from, to).then((usd_rates)=>{
                    // for each rate received, push it in cassandra
                    Object.keys(usd_rates).forEach((key)=>{
                        this._cassandraWriter.writeExchangeRates(keyspace, [{
                            date: key,
                            eur: eur_rates[key],
                            usd: usd_rates[key]
                        }]).catch(this._logErrors);
                    });
                }).catch(this._logErrors);
            }).catch(this._logErrors);
        }).catch(this._logErrors);
    }

    _getRatesFromCoindesk(symbol, from, to) {
        return new Promise((resolve,reject)=>{
            let useragent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0";
            let url = "https://api.coindesk.com/v1/bpi/historical/close.json"+
            "?index=USD&currency="+symbol+
            "&start="+from+"&end="+to;
            console.log(url);
            request(url, {headers: {
                "User-Agent": useragent
            }}, (err, resp, body)=>{
                if(err) {
                    reject(err);
                    return;
                }
                if(resp.statusCode!=200) {
                    reject("Unexpected status code:"+resp.statusCode);
                    return;
                }
                resolve(JSON.parse(body).bpi);
            });     
        });
    }

    _getLastAvailableBlock() {
        return new Promise((resolve,reject)=>{
            // get list of bitcoin nodes available
            var CRYPTO_CLIENTS = process.env.CRYPTO_CLIENTS;
            var crypto_clients_endpoints = CRYPTO_CLIENTS.split(",");
            var height = Infinity;
            let replyCount = 0;
            // define callback
            let parseHeightCallback = (err, response, body) => {
                // error handling
                if(err || response.statusCode != 200 || JSON.parse(body).error != null) {
                    this._logErrors("Can't reach some bitcoin clients");
                    if(err) this._logErrors(err);
                    height = -1;
                // update min height
                } else {
                    height = Math.min(height, JSON.parse(body).result);
                }
                // test if we are done
                replyCount++;
                if(replyCount>=crypto_clients_endpoints.length) {
                    resolve(height);
                }
            };
            // for each bitcoin client
            for(let i=0;i<crypto_clients_endpoints.length;i++) {
                // get the number of blocks
                var dataString = "{\"jsonrpc\":\"1.0\",\"id\":\"curltext\",\"method\":\"getblockcount\",\"params\":[]}";
                var options = {
                    url: `http://${process.env.CRYPTO_CLIENTS_LOGIN}:${process.env.CRYPTO_CLIENTS_PWD}@${crypto_clients_endpoints[i]}/`,
                    method: "POST",
                    headers: {"content-type": "text/plain;"},
                    body: dataString
                };
                request(options, parseHeightCallback);
            }
        });
    }

    _getMonitoredKeyspaces() {
        return new Promise((resolve,reject)=>{
            // get the set of keyspaces monitored
            this._redisClient.smembers(this._currency.toUpperCase()+"::monitored-keyspaces", (errSME,resSME)=>{
                // error handling
                if(errSME) {
                    reject(errSME);
                    return;
                }
                // if there are no, return empty list
                if(resSME==null || (Array.isArray(resSME)==true && resSME.length==0)) {
                    resolve([]);
                    return;
                }
                // if there are, for each, add a hgetall call inside a request group
                let mult = this._redisClient.multi();
                for(let i=0;i<resSME.length;i++) {
                    mult.hgetall(this._currency.toUpperCase()+"::monitored::"+resSME[i]);
                }
                // execute the mult and return the array of objects
                mult.exec((errMul, resMul)=>{
                    if(errMul) {
                        reject(errMul);
                        return;
                    }

                    resolve(resMul);
                });
            });
        });
    }

    _checkActiveJobLiveliness() {
        return new Promise((resolve, reject)=>{
            // get the list of responding workers
            this._getActiveWorkers().then((workers)=>{
                // get the list of doing jobs
                this._redisClient.lrange(""+this._currency+"::jobs::doing", 0, -1, (errLR1,resLR1)=>{
                    // error handling
                    if(errLR1) {
                        reject("REDIS ERROR (LR1): "+errLR1);
                        return;
                    }

                    // if there are no job currently being processed, do nothing
                    if(resLR1==null || (Array.isArray(resLR1)==true && resLR1.length==0)) {
                        resolve();
                        return;
                    }

                    let abandonedJobs = [];

                    // now we iterate over job 
                    for(let i=0;i<resLR1.length;i++) {
                        // parse associated worker
                        let id = resLR1[i].split("::")[2];
                        // if its worker has not response to calls
                        if(workers.includes(id)==false) {
                            // save it to move it later
                            this._debug("Job "+resLR1[i]+" has no responding worker, moving back to todo.");
                            abandonedJobs.push(resLR1[i]);
                        }
                    }

                    // if we have found no job with dead worker nothing to do
                    if(abandonedJobs.length==0) {
                        resolve();
                        return;
                    }

                    // bulk redis calls
                    let mult = this._redisClient.multi();

                    // we must now move blocks with dead workers back to todo list
                    for(let i=0;i<abandonedJobs.length;i++) {
                        this._debug("Moving back orphaned job to todo list: "+abandonedJobs[i]);
                        mult.lrem(""+this._currency+"::jobs::doing", 1, abandonedJobs[i]);
                    }

                    mult.exec((errMult,resMult)=>{
                        if(errMult) {
                            reject("REDIS ERROR (mult in master's _checkActiveJobLivelyness):"+errMult);
                            return;
                        }

                        // now push back to todo
                        let mult2 = this._redisClient.multi();

                        // remove worker id and push
                        for(let i=0;i<abandonedJobs.length;i++) {
                            let splittedJob = abandonedJobs[i].split("::");
                            mult2.lpush(""+this._currency+"::jobs::todo", ""+splittedJob[0]+"::"+splittedJob[1]+"::"+splittedJob[3]);
                        }

                        // execute the final mult
                        mult2.exec((errMul2,resMul2)=>{
                            if(errMul2) {
                                reject("REDIS ERROR (Master's _checkActiveJobLivelyness mult 2): job may have been lost: "+errMul2);
                                return;
                            }
                            resolve();
                        });
                    });
                });
            }).catch(reject);
        });
    }

    _getActiveWorkers() {
        return new Promise((resolve, reject)=>{
            // reset the array that store workers ids
            this._workersIds = [];
            // send a ping message
            this._sendMessage("CALLROLL", "request");
            // wait for responses
            setTimeout(()=>{
                resolve(this._workersIds);
            }, CALLROLL_DELAY);
        });
    }

    _writeCryptoClients(list) {
        // for each crypto client
        for(let i=0;i<list.length;i++) {
            // remove and set
            this._redisClient.lrem(this._currency+"::node-clients", 1, list[i],(errLR,resLR)=>{
                // error handling
                if(errLR) {
                    this._logErrors("REDIS ERROR(MasterRole): error while replacing crypto client:"+errLR);
                    return;
                }
                // push the new one
                this._redisClient.lpush(this._currency+"::node-clients", list[i], (errLP,resLP)=>{
                    if(errLP) {
                        this._logErrors("REDIS ERROR(MasterRole): error while replacing crypto client:"+errLP);
                    }
                });
            });
        }
    }

    // stop listening to redis, stop servicing as master
    stopMaster() {
        // NOTE: no need to drop the work, the lack of ping response will make master reset jobs
        this._subClient.unsubscribe( ""+this._currency + "::" + "job-scheduling");
        // close redis clients
        this._subClient.quit();
        this._redisClient.quit();
        this._stopped = true;
        // master will only exit if the entire program does, so no point in cleaning intervals
    }
}

module.exports = {MasterRole};
