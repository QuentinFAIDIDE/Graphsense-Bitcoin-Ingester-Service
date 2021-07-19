var redis = require("redis");
var randomstring = require("randomstring");
var request =require("request");
var { CassandraWriter } = require("./cassandra-writer.js");

var MASTER_JOBCHECK_INTERVAL = 30000;
var CALLROLL_DELAY = 3000;
var BLOCK_BATCH_SIZE = 75;
var MIN_RATE_DATE = "2010-10-17";

var MAX_TODO_STACK_LEN = 15;

var RATE_WRITING_LOCK_TIMEOUT = 60000*10;

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

        this._rateWritingLocks = {};

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
                        this._logErrors(errLLE);
                        return;
                    }
                    // get all keyspaces JSONs objects on watch list
                    this._getMonitoredKeyspaces().then((keyspaces)=>{

                        let totalNbKeyspaces = keyspaces.length;
                        let processedKeyspaces = 0;
                        
                        // create a mult to bulk redis calls
                        let mult = this._redisClient.multi();

                        let processingDoneCallback = ()=>{
                            processedKeyspaces++;
                            if(processedKeyspaces>=totalNbKeyspaces) {
                                // execute the db calls
                                mult.exec((errMult, resMult)=>{
                                    if(errMult) {
                                        this._logErrors(errMult);
                                    } else {
                                        this._debug("Succesfully updated monitored keyspaces and dead workers");
                                    }
                                });
                            }
                        };

                        this._debug("Keyspaces monitored found: "+JSON.stringify(keyspaces));

                        // for each one, if it has been marked as to update continously
                        for(let i=0; i<keyspaces.length;i++) {

                            // ignore the keyspace if it's marked broken
                            if(Object.prototype.hasOwnProperty.call(keyspaces[i], "broken")==true && keyspaces[i].broken=="true") {
                                continue;
                            }

                            // if keyspace has not been marked as "not to feed in" with feedFrom as -1
                            if(keyspaces[i].feedFrom!=-1) {

                                // first, check if keyspace has filled block range further enough to pop new enrich jobs (requires to have all previous utxo)
                                this._updateLastFilledBlockAndPopEnrichJobs(keyspaces[i]).then(()=>{

                                    this._updateLastEnrichedBlockAndStatistics(keyspaces[i]).then(()=>{

                                        // if last block availabl minus confirm delay is bigger than last keyspace block
                                        if(Number(keyspaces[i].lastQueuedBlock) < Number(maxheight) - Number(keyspaces[i].delay)) {
                                            // push a job for the new block
                                            let range = [Number(keyspaces[i].lastQueuedBlock)+1 ,Number(maxheight) - Number(keyspaces[i].delay)];
                                            
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

                                            // update the last queued block for the keyspace
                                            mult.hset(this._currency.toUpperCase()+"::monitored::"+keyspaces[i].name, "lastQueuedBlock", furthest_block_sent);
                                        }
                                        // detect lock to prevent rate writing race conditions 
                                        let ratesWriting = false;
                                        if(this._rateWritingLocks.hasOwnProperty(keyspaces[i].name)==true) {
                                            // if lock is expired
                                            if((Date.now()-this._rateWritingLocks[keyspaces[i].name])>RATE_WRITING_LOCK_TIMEOUT) {
                                                // ignore the lock and delete it
                                                delete this._rateWritingLocks[keyspaces[i].name];
                                            // if lock is still valid, set bool as true
                                            } else {
                                                ratesWriting = true;
                                            }
                                        }
                                        // if the lock is set for the keyspace
                                        if(ratesWriting==true) {
                                            // return and ignore the rate writing part
                                            processingDoneCallback();
                                            return;
                                        }
                                        // if rates were not set
                                        if(keyspaces[i].hasOwnProperty("lastRatesWritten")==false) {
                                            // get yesterdays date in YYYY-MM-DD format
                                            let yestedayTxt = this._getYesterdayDateTxt();
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
                                                // call the function to update rates in this namespace
                                                this._updateRatesForKeyspaceForRange(keyspaces[i].name, lastRatesWritten, yestedayTxt);
                                            }
                                        }
                                        processingDoneCallback();
                                        return;
                                    }).catch(this._logErrors);
                                }).catch(this._logErrors);
                            }
                        }

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
        if(String(day).length==1) {
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
        // set the rate update lock
        this._rateWritingLocks[keyspace] = Date.now();
        // debug logs...
        this._debug("Updating rates from "+from+" to "+to);
        // prepare the keyspace
        this._cassandraWriter.prepareForKeyspace(keyspace).then(()=>{
            // once its ready, get the btc rates
            this._getRatesFromCoindesk("EUR" ,from, to).then((eur_rates)=>{
                this._getRatesFromCoindesk("USD" ,from, to).then((usd_rates)=>{
                    // used to keep track of async operations
                    let toWrite=0;
                    let success=0;
                    let failures=0;
                    // for each rate received, push it in cassandra
                    Object.keys(usd_rates).forEach((key)=>{
                        toWrite++;
                        this._cassandraWriter.writeExchangeRates(keyspace, [{
                            date: key,
                            eur: eur_rates[key],
                            usd: usd_rates[key]
                        }]).then(()=>{
                            // monitor succesfull writes
                            success++;
                            // if done
                            if((success)>=toWrite) {
                                // we are done and will write the lastRatesWritten
                                this._redisClient.hset(this._currency.toUpperCase()+"::monitored::"+keyspace, "lastRatesWritten", to);
                                // liberate the lock
                                delete this._rateWritingLocks[keyspace];
                            // if done with errors
                            } else if((success+failures)>=toWrite) {
                                // liberate the lock
                                delete this._rateWritingLocks[keyspace];
                            }
                        }).catch((err)=>{
                            failures++;
                            // monitor failed writes
                            this._logErrors(err);
                            // if done
                            if((success+failures)>=toWrite) {
                                // liberate the lock
                                delete this._rateWritingLocks[keyspace];
                            }
                        });
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

    // this function get all block range written and update the lastFilledBlock value
    // The fill job push their range to a list that we read to know what's the last block written that has all previous ones filled
    // This is because the enrich job needs to find origin tx input, and therefore must have all unspent UTXO available (and all block before filled)
    _updateLastFilledBlockAndPopEnrichJobs(keyspaceobj) {
        return new Promise((resolve,reject)=>{
            // get the list of blocks range filled, no more than the 600 first ones
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspaceobj.name, "0", "600", (errLR, resLR)=>{
                // error handling
                if(errLR) {
                    reject(errLR);
                    return;
                }
                // if no new range was filled, abort
                if(resLR==null || (Array.isArray(resLR)==true && resLR.length==0)) {
                    resolve();
                    return;
                }
                // parse ranges
                let ranges = [];
                for(let i=0;i<resLR.length;i++) {
                    let splittedString = resLR[i].split(",");
                    ranges.push([Number(splittedString[0]), Number(splittedString[1])]);
                }
                // sort em by range beginning order 
                ranges.sort((a,b)=>{
                    if(a[0]<=b[0])return -1;
                    else return 1;
                });

                // ranges to simply clear
                let ranges_to_clear = [];

                // increment over each job to increment the last block where all previous are filled
                let lastFilledBlock = Number(keyspaceobj.lastFilledBlock);
                let allBlocksBeforeAreFilled = true;
                let fillJobFinishedIterator = 0;
                while(allBlocksBeforeAreFilled==true && fillJobFinishedIterator<ranges.length) {
                    // if the next range is available without a gap
                    if(ranges[fillJobFinishedIterator][0]==(lastFilledBlock+1)) {
                        // change lastFilledBlock and iterate over next one
                        lastFilledBlock=ranges[fillJobFinishedIterator][1];
                        fillJobFinishedIterator++;
                    // if a race condition hapenned and we have already done this range
                    } else if (ranges[fillJobFinishedIterator][1]<(lastFilledBlock+1)) {
                        // mark the range as needing to be cleared from list
                        ranges_to_clear.push(ranges[fillJobFinishedIterator]);
                        // pop this out and repeat without incrementing
                        ranges.splice(fillJobFinishedIterator,1);
                    // if not, break out of the loop since we have a gap
                    } else {
                        allBlocksBeforeAreFilled=false;
                    }
                }

                // if nothing changed, return 
                if(fillJobFinishedIterator==0) {
                    resolve();
                    return;
                }

                // if we have updated our lastFilledBlock, we will write it and delete job ranged used
                let multi = this._redisClient.multi();
                let jobname = "";
                
                for(let i=0;i<fillJobFinishedIterator;i++) {
                    // add all jobs used to the lrem
                    multi.lrem(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspaceobj.name, 1, ranges[i][0]+","+ranges[i][1]);
                    // push em as enrich jobs for our keyspace
                    // we push filling job to the right of the queue, but with our enrich job, we push to the left
                    // so that we jump the queue that is poped from the left when workers pull work
                    // this is to ensure we have the least data possible in the redis cache 
                    jobname = keyspaceobj.name+"::ENRICH_BLOCK_RANGE::"+ranges[i][0]+","+ranges[i][1];
                    // push the job in the redis bulk call objects
                    multi.lpush(this._currency.toUpperCase()+"::jobs::todo", jobname);
                }

                // delete ranges to clear
                for(let i=0;i<ranges_to_clear.length;i++) {
                    multi.lrem(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspaceobj.name, 1, ranges_to_clear[i][0]+","+ranges_to_clear[i][1]);
                }

                // let's now update the lastFilledBlock variable
                multi.hset(this._currency.toUpperCase()+"::monitored::"+keyspaceobj.name, "lastFilledBlock", lastFilledBlock);

                multi.exec((errMul,resMul)=>{
                    if(errMul) {
                        reject(errMul);
                        return;
                    }
                    resolve();
                });
            });
        });
    }

    // function that pull enriched ranges list to update the last enriched block and write the keyspace statistics
    _updateLastEnrichedBlockAndStatistics(keyspaceobj) {
        return new Promise((resolve,reject)=>{

            // pull 600 leftmost (eg oldest) ranges enriched to check furthest block below which everything is filled
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::"+keyspaceobj.name+"::enriched-ranges", 0, 600, (errLR, resLR)=>{
                // error handling
                if(errLR) {
                    reject(errLR);
                    return;
                }
                // if no new range was filled, abort
                if(resLR==null || (Array.isArray(resLR)==true && resLR.length==0)) {
                    resolve();
                    return;
                }
                // parse ranges
                let ranges = [];
                for(let i=0;i<resLR.length;i++) {
                    let splittedString = resLR[i].split(",");
                    ranges.push([Number(splittedString[0]), Number(splittedString[1])]);
                }
                // sort em by range beginning order 
                ranges.sort((a,b)=>{
                    if(a[0]<=b[0])return -1;
                    else return 1;
                });

                let ranges_to_clear = [];

                // increment over each job to increment the last block where all previous are enriched
                let lastEnrichedBlock = -1;
                if(keyspaceobj.hasOwnProperty("lastEnrichedBlock")==true) {
                    lastEnrichedBlock = Number(keyspaceobj.lastEnrichedBlock);
                }
                let allBlocksBeforeAreEnriched = true;
                let enrichJobFinishedIterator = 0;
                while(allBlocksBeforeAreEnriched==true && enrichJobFinishedIterator<ranges.length) {
                    // if the next range is available without a gap
                    if(ranges[enrichJobFinishedIterator][0]==(lastEnrichedBlock+1)) {
                        // change lastEnrichedBlock and iterate over next one
                        lastEnrichedBlock=ranges[enrichJobFinishedIterator][1];
                        enrichJobFinishedIterator++;
                    } else if (ranges[enrichJobFinishedIterator][1]<(lastEnrichedBlock+1)) {
                        // mark the range as needing to be cleared from list
                        ranges_to_clear.push(ranges[enrichJobFinishedIterator]);
                        // pop this out and repeat without incrementing
                        ranges.splice(enrichJobFinishedIterator,1);
                        // if not, break out of the loop since we have a gap
                    } else {
                        allBlocksBeforeAreEnriched=false;
                    }
                }

                // if nothing changed, return 
                if(enrichJobFinishedIterator==0) {
                    resolve();
                    return;
                }

                // if we have updated our lastEnrichedBlock, we will add all the tx an block counts with redis INCR
                // this mult will first get the tx counts per job
                let multi = this._redisClient.multi();
                let jobname = "";
                
                for(let i=0;i<enrichJobFinishedIterator;i++) {
                    // get tx counts for job
                    multi.get(""+this._currency.toUpperCase()+"::job_stats::"+ keyspaceobj.name+"::"+ranges[i][0]+","+ranges[i][1]);
                }

                // let's also delete rabges that potential race condition caused
                for(let i=0;i<ranges_to_clear.length;i++) {
                    multi.lrem(""+this._currency.toUpperCase()+"::"+keyspaceobj.name+"::enriched-ranges", 1, ""+ranges_to_clear[i][0]+","+ranges_to_clear[i][1]);
                }

                multi.exec((errMUL1, resMUL1)=>{
                    // error handling (abort operations on redis errors)
                    if(errMUL1) {
                        reject(errMUL1);
                        return;
                    }
                    // create a new multi
                    let multIncr = this._redisClient.multi();
                    // for each txCount we got for all jobs
                    for(let i=0;i<enrichJobFinishedIterator;i++) {
                        // just in case a tx count was missing for a job
                        if(resMUL1[i]==null) {
                            // TODO: mark keyspace as broken in case this error happens one day
                            reject(new Error("Tx count was missing for a job, can't write job statistics !"));
                            return;
                        }
                        // if it's there, increment block number
                        multIncr.incrby(this._currency.toUpperCase()+"::"+keyspaceobj.name+"::block-count", ranges[i][1]+1-ranges[i][0]);
                        // also increment tx count
                        multIncr.incrby(this._currency.toUpperCase()+"::"+keyspaceobj.name+"::tx-count", resMUL1[i]);
                        // finally, remove the range from the enriched job list
                        multIncr.lrem(""+this._currency.toUpperCase()+"::"+keyspaceobj.name+"::enriched-ranges", 1, ""+ranges[i][0]+","+ranges[i][1]);
                    }

                    // also, write the hash for last enriched block
                    multIncr.hset(this._currency.toUpperCase()+"::monitored::"+keyspaceobj.name, "lastEnrichedBlock", lastEnrichedBlock);

                    // only after we can get our total block and tx counts
                    multIncr.get(this._currency.toUpperCase()+"::"+keyspaceobj.name+"::block-count");
                    multIncr.get(this._currency.toUpperCase()+"::"+keyspaceobj.name+"::tx-count");

                    
                    // execute it
                    multIncr.exec((errMUL2, resMUL2)=>{
                        if(errMUL2) {
                            this._logErrors("Failure when trying to update keyspace statistics.");
                            reject(errMUL2);
                            return;
                        }
                        // let's get the count in the response object (last two ones)
                        let block_count = resMUL2[resMUL2.length-2];
                        let tx_count = resMUL2[resMUL2.length-1];
                        // now make the calls to the cassandraWriter class to write the statistics
                        this._cassandraWriter.writeKeyspaceStatistics(keyspaceobj.name, block_count, tx_count);
                        // do not wait for cassandra writes, just return at this point
                        resolve();
                        return;
                    });
                });
            });
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
