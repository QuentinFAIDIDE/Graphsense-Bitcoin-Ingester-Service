var redis = require("redis");
var randomstring = require("randomstring");
var request =require("request");
var { CassandraWriter } = require("./cassandra-writer.js");

var MASTER_JOBCHECK_INTERVAL = 30000;
var CALLROLL_DELAY = 10000;
var BLOCK_BATCH_SIZE = 100;
var MIN_RATE_DATE = "2010-10-17";



// the maximum number of jobs in the filled range stack before refusing to put further filling jobs
// todo: implement that
var MAX_FILL_ENRICH_SPREAD = 1000;

var MAX_TODO_STACK_LEN = 150;

var RATE_WRITING_LOCK_TIMEOUT = 60000*10;


// the min height for us to start scaling size of ingested ranges
var DEFAULT_JOB_RANGE = 50;
// the maximum time a job can be in the todo queue before clearing it
var JOB_LIFE_TIMEOUT = 60000*40;

var MAXIMUM_MASTER_CHECK_TIME = 7*60000;

// detect if redis password setting is used
var USE_REDIS_PASSWORD = !(typeof process.env.REDIS_PASSWORD == "undefined");
var REDIS_PASSWORD = process.env.REDIS_PASSWORD;

class MasterRole {
    /*
    Class that implements master role
    (push work to list, watch keyspaces, workers, etc...)
  */

    constructor(redisHost, redisPort, symbol, logMessage, logErrors, debug) {

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
        this._workersDelay = [];

        this._rateWritingLocks = {};

        this._cassandraWriter = new CassandraWriter(redisHost,redisPort, symbol, logMessage, logErrors, debug);

        this._recentlySeenFailedJobs = [];

        // per keyspace list of submitted jobs with dates used to check for timeout (lost ones)
        this._fillingJobsQueued = {};
        this._enrichJobsQueued = {};

        // list of last block speed to average to help scale job sizes
        this._lastBlockSpeeds = [];

        // the size of the range we ingest per job
        this._blockRange = DEFAULT_JOB_RANGE;

        // bool to prevent race condition on exceptional outages
        this._isRunningRoutine = false;

        // used to cache unresponsive workers jobs for trying to reach em again
        this._unresponsiveWorkerJobs = [];

        this._lastCallRollTime = Date.now();
    }

    // PRIVATE METHODS
    /* act on received messages on the redis channel for replicas scheduling */
    /* message is formatted as so: "[CURRENCY]::[TIMESTAMP]::[IDENTIFIER_EMITTER]::[ACTION]::[PARAMETER1,PARAMETER2,etc..]*/
    _parseChannelMessage(msg) {
        try {
            // parsing the message parameters
            let messageContent = String(msg).split("::");
            let parameters = messageContent[4].split(",");
            // if someone is responding to a call roll
            if(messageContent[3]=="CALLROLL" && parameters[0]=="response") {
                // it's indeed ours
                if(parameters[2]==this._identifier) {
                    this._workersIds.push(parameters[1].trim());
                    let responsetime = (Date.now()-this._lastCallRollTime);
                    this._workersDelay.push(responsetime);
                    this._debug("Replica "+parameters[1].trim()+" took "+responsetime+" ms to reply.");
                }
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

        // logs a redis response time every 5 seconds, and check if the master routines are still executing
        setInterval(()=>{
            let timeStart = Date.now();
            this._redisClient.lrange(this._currency.toUpperCase()+"::metrics-data::redis-timeouts", 0, 1, (errLR, resLR)=>{
                let timeTaken = Date.now() - timeStart;
                this._redisClient.lpush(this._currency.toUpperCase()+"::metrics-data::redis-timeouts", ""+timeTaken);
            });
            // kill this master if its routines are in an obvious blocked state
            if(this._lastMasterCheckupDate!=0 && (Date.now()-this._lastMasterCheckupDate)>MAXIMUM_MASTER_CHECK_TIME ) {
                this._logErrors("Last master check was too long ago, dropping this master for a new one.");
                process.exit(1);
            }
        }, 5000);
    }

    _scaleBlockRange() {
        // this feature was disable due to the delay between currently measured block rate and the one in the future when it will get exec
        // instead, we are scaling with predefined sizes based on height of generated job
        return;
    }

    _runJobCheck() {
        // help prevents race conditions
        if(this._isRunningRoutine==true) {
            return;
        } else {
            this._isRunningRoutine=true;
        }
        let startedAt = Date.now();
        this._debug("Master run liveliness probe for workers");
        // are all jobs in the "doing" column have responding workers ?
        this._checkActiveJobLiveliness().then(()=>{
            // get last block available
            this._getLastAvailableBlock().then((maxheight)=>{

                // error handling
                if(maxheight==-1) {
                    // error was already logged
                    // simply returning
                    this._isRunningRoutine=false;
                    return;
                }
                
                this._redisClient.llen(""+this._currency.toUpperCase()+"::jobs::todo", (errLLE, todoStackLength)=>{
                    if(errLLE) {
                        this._isRunningRoutine=false;
                        this._logErrors(errLLE);
                        return;
                    }
                    // get all keyspaces JSONs objects on watch list
                    this._getMonitoredKeyspaces().then((keyspaces)=>{

                        // check the error stack and either repost job or mark keyspace as broken
                        this._checkErrorStack().then(()=>{

                            // check the list of posted jobs to delete those who are done and check the timed outs
                            this._checkTimedOutJobs().then(()=>{


                                this._lastMasterCheckupDate=Date.now();

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
                                            }
                                            // we remove the time to wait for replcia responses to have more accurate metrics
                                            let timeTaken = Date.now() - startedAt - CALLROLL_DELAY;
                                            this._redisClient.publish(this._currency.toUpperCase()+"::metrics", "master-routine-time: "+timeTaken);
                                            this._redisClient.set(this._currency.toUpperCase()+"::metrics::master-routine-time", timeTaken);
                                            this._updateMetrics();
                                            this._debug("Succesfully updated monitored keyspaces and dead workers");
                                            this._isRunningRoutine=false;
                                        });
                                    }
                                };

                                this._debug("Keyspaces monitored found: "+JSON.stringify(keyspaces));

                                // terminate if nothing to check
                                if(keyspaces.length==0) {
                                    processingDoneCallback();
                                    return;
                                }

                                // for each one, if it has been marked as to update continously
                                for(let i=0; i<keyspaces.length;i++) {

                                    // ignore the keyspace if it's marked broken
                                    if(Object.prototype.hasOwnProperty.call(keyspaces[i], "broken")==true && keyspaces[i].broken=="true") {
                                        processingDoneCallback();
                                        continue;
                                    }

                                    // if keyspace has not been marked as "not to feed in" with feedFrom as -1
                                    if(keyspaces[i].feedFrom!=-1) {

                                        // first, check if keyspace has filled block range further enough to pop new enrich jobs (requires to have all previous utxo)
                                        this._updateLastFilledBlockAndPopEnrichJobs(keyspaces[i]).then(()=>{

                                            this._updateLastEnrichedBlockAndStatistics(keyspaces[i]).then(()=>{

                                                // read the feedUntill parameter if exists, and set it to a arbitrarly high value else
                                                let feedUntill = -1;
                                                if(Object.prototype.hasOwnProperty.call(keyspaces[i], "feedUntill")==true) {
                                                    feedUntill = Number(keyspaces[i].feedUntill);
                                                } else {
                                                    feedUntill = Number(maxheight)+1;
                                                }

                                                // if last block availabl minus confirm delay is bigger than last written keyspace block
                                                // and that the last written block is below the user defined maxmium block to ingest
                                                if(Number(keyspaces[i].lastQueuedBlock) < Number(maxheight) - Number(keyspaces[i].delay)
                                                && Number(keyspaces[i].lastQueuedBlock) < feedUntill) {
                                                    // define the missing range to cover
                                                    let range = [Number(keyspaces[i].lastQueuedBlock)+1 ,Number(maxheight) - Number(keyspaces[i].delay)];

                                                    // if the range upper part is above the user defined maximum, truncate it
                                                    if(range[1]>feedUntill) {
                                                        range[1]=feedUntill;
                                                    }
                                                    
                                                    let jobname;

                                                    let tasksPending = todoStackLength;
                                            
                                                    // split job into subtask of blck size
                                                    let furthest_block_sent = Number(range[0]-1);
                                                    let end = Number(range[1]);
                                                    // update to appropriate split size
                                                    let split_size = this._rangeSizeForHeight(furthest_block_sent+1);
                                                    while ((furthest_block_sent + split_size) <= end && tasksPending<MAX_TODO_STACK_LEN) {
                                                        tasksPending++;
                                                        // sending job for subrange
                                                        jobname = keyspaces[i].name+"::FILL_BLOCK_RANGE::"+
                                                            (furthest_block_sent+1)+","+String(furthest_block_sent + split_size);
                                                        // push the job in the redis bulk call objects
                                                        mult.rpush(this._currency.toUpperCase()+"::jobs::todo", jobname);
                                                        mult.rpush(this._currency.toUpperCase()+"::jobs::posted", ""+Date.now()+"::"+jobname);
                                                        // increment to next batch
                                                        furthest_block_sent += split_size;

                                                        // update split size
                                                        split_size = this._rangeSizeForHeight(furthest_block_sent+1);
                                                    }

                                                    // if there is a remainder, send it as well
                                                    if (furthest_block_sent < end && tasksPending<MAX_TODO_STACK_LEN) {
                                                        // sending job for subrange
                                                        jobname = keyspaces[i].name+"::FILL_BLOCK_RANGE::"+
                                                            (furthest_block_sent+1)+","+(end);
                                                        furthest_block_sent = end;
                                                        // push the job in the redis bulk call objects
                                                        mult.rpush(this._currency.toUpperCase()+"::jobs::todo", jobname);
                                                        mult.rpush(this._currency.toUpperCase()+"::jobs::posted", ""+Date.now()+"::"+jobname);
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
                                            }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
                                        }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
                                    }
                                }
                            }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
                        }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
                    }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
                });
            }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
        }).catch((err)=>{this._logErrors(err);this._isRunningRoutine=false;});
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

    // this function get all block range "filled", generate a job to "enrich" these ranges and update the lastFilledBlock value
    // The fill job push their range to a list that we read to know what's the last block written that has all previous ones filled
    // This is because the enrich job needs to find origin tx input, and therefore must have all unspent UTXO available (and all block before filled)
    _updateLastFilledBlockAndPopEnrichJobs(keyspaceobj) {
        return new Promise((resolve,reject)=>{
            // get the list of blocks range filled, no more than the 700 first ones
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspaceobj.name, "0", -1, (errLR, resLR)=>{
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
                if(fillJobFinishedIterator==0 && ranges_to_clear.length==0) {
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
                    multi.rpush(this._currency.toUpperCase()+"::jobs::posted", ""+Date.now()+"::"+jobname);
                }

                // delete ranges to clear
                for(let i=0;i<ranges_to_clear.length;i++) {
                    multi.lrem(""+this._currency.toUpperCase()+"::filled-ranges::"+keyspaceobj.name, 1, ranges_to_clear[i][0]+","+ranges_to_clear[i][1]);
                    this._logErrors("Cleared duplicated range for filling: "+ranges_to_clear[i][0]+","+ranges_to_clear[i][1]);
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

    // will read error stack, compare jobs to see if they have already failed before, and either repush to todo stack or mark keyspace as broken
    _checkErrorStack() {
        return new Promise((resolve, reject)=>{
            // get the first 50 elements of the error stack
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::jobs::errors", 0, -1, (errLR, resLR)=>{
                if(errLR) {
                    this._logErrors("Error at redis lrange in _checkErrorStack");
                    reject(errLR);
                    return;
                }

                // if no error, do nothing
                if(resLR==null || (Array.isArray(resLR)==true && resLR.length==0)) {
                    resolve();
                    return;
                }

                let jobShortnames = [];

                // for each error, if it's included in the memory of failed jobs, abort and mark keyspace as broken
                // remainder error format: ingester_test::ENRICH_BLOCK_RANGE::1626907730450CzJNqqXh::363000,363074
                for(let i=0;i<resLR.length;i++) {
                    // get the job field
                    let jobFields = resLR[i].split("::");
                    // put it back in the "todo" format for jobs (without owner identifier)
                    jobShortnames.push(jobFields[0]+"::"+jobFields[1]+"::"+jobFields[3]);
                    // if it recently failed already
                    if(this._recentlySeenFailedJobs.includes(jobShortnames[i])==true) {
                        // mark keyspace as broken
                        this._redisClient.hset(this._currency.toUpperCase()+"::monitored::"+jobFields[0], "broken", "true", (errHS, resHS)=>{
                            if(errHS) {
                                this._logErrors("Redis error while marking keyspace as broken because of repeated failed job.");
                                reject(errHS);
                                return;
                            }

                            resolve();
                        });
                        return;
                    } else {
                        // if not recently seen, save it here in case we find it later
                        this._recentlySeenFailedJobs.push(jobShortnames[i]);
                        // trim the list if too big, and issue an alert
                        if(this._recentlySeenFailedJobs.length>10000) {
                            this._logErrors("Master has seen more than 10000 failed jobs. It is either due to a bug or to stressing too much cassandra or redis instances.");
                            this._recentlySeenFailedJobs.splice(0, this._recentlySeenFailedJobs.length-10000);
                        }
                    }
                }

                // if we reached here, there were no faulty job uncovered by the "recently seen" test
                // now for each job, push it back to todo list
                let multi = this._redisClient.multi();
                for(let i=0;i<resLR.length;i++) {
                    multi.lpush(this._currency.toUpperCase()+"::jobs::todo", jobShortnames[i]);
                    multi.lrem(this._currency.toUpperCase()+"::jobs::errors", 1, resLR[i]);
                }

                // finally execute the redis multi and resolve
                multi.exec((errMul, resMul)=>{
                    if(errMul) {
                        reject(errMul);
                        return;
                    }

                    this._logErrors("Master recovered first time failed jobs.");

                    resolve();
                    return;
                });
            });
        });
    }

    // function that pull enriched ranges list to update the last enriched block and write the keyspace statistics
    _updateLastEnrichedBlockAndStatistics(keyspaceobj) {
        return new Promise((resolve,reject)=>{

            // pull 600 leftmost (eg oldest) ranges enriched to check furthest block below which everything is filled
            this._redisClient.lrange(""+this._currency.toUpperCase()+"::"+keyspaceobj.name+"::enriched-ranges", 0, -1, (errLR, resLR)=>{
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
                if(enrichJobFinishedIterator==0 && ranges_to_clear.length==0) {
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
                    this._logErrors("Cleared duplicated range for enrichment: "+ranges_to_clear[i][0]+","+ranges_to_clear[i][1]);
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
                        // drop a few metrics
                        this._redisClient.publish(this._currency.toUpperCase()+"::metrics", "lasEnrichedBlock: "+lastEnrichedBlock);
                        this._redisClient.publish(this._currency.toUpperCase()+"::metrics", "blocksInCassandra: "+block_count);
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
                this._debug("workers responding to master: "+JSON.stringify(workers));
                // get the list of doing jobs
                this._redisClient.lrange(""+this._currency.toUpperCase()+"::jobs::doing", 0, -1, (errLR1,resLR1)=>{
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

                    // we will wait for next call to retry all failing workers
                    // and we also retry failed workers from previous call to eventually delete em

                    let abandonedJobs = [];

                    // now we iterate over job 
                    for(let i=0;i<resLR1.length;i++) {
                        // parse associated worker
                        let id = resLR1[i].split("::")[2];
                        // if its worker has not response to calls
                        if(workers.includes(id.trim())==false) {
                            // save it to move it later
                            this._logErrors("Job "+resLR1[i]+" has no responding worker under "+CALLROLL_DELAY+"ms, will test workers a second time and eventually clear them.");
                            abandonedJobs.push(resLR1[i]);
                        }
                    }

                    let jobsToDrop = [];
                    // for each job previously marked as unresponsive
                    for(let i=0;i<this._unresponsiveWorkerJobs.length;i++) {
                        // if it is still in the todo list (could have beeen cleared for timing out in between two calls to this function)
                        if(resLR1.includes(this._unresponsiveWorkerJobs[i])==true) {
                            // parse associated worker id
                            let id = this._unresponsiveWorkerJobs[i].split("::")[2];
                            // if its worker has not response to calls
                            if(workers.includes(id)==false) {
                                this._logErrors("Job "+this._unresponsiveWorkerJobs[i]+" failed a second time, dropping job.");
                                jobsToDrop.push(this._unresponsiveWorkerJobs[i]);
                            }
                        }
                    }


                    let jobsToCheckLater = [];

                    // put only the new failures in the list
                    for(let i=0;i<abandonedJobs.length;i++) {
                        if(this._unresponsiveWorkerJobs.includes(abandonedJobs[i])==false) {
                            jobsToCheckLater.push(abandonedJobs[i]);
                        }
                    }
                    // save the new failures as the ones to check at next turn
                    this._unresponsiveWorkerJobs = jobsToCheckLater;

                    // if we have found no job with dead worker twice, nothing to do
                    if(jobsToDrop.length==0) {
                        resolve();
                        return;
                    }

                    // bulk redis calls
                    let mult = this._redisClient.multi();

                    // we must now move blocks with dead workers back to todo list
                    for(let i=0;i<jobsToDrop.length;i++) {
                        this._logErrors("Moving back orphaned job to todo list: "+jobsToDrop[i]);
                        mult.lrem(""+this._currency+"::jobs::doing", 1, jobsToDrop[i]);
                    }

                    // get the list of posted jobs to update the dates
                    mult.lrange(this._currency.toUpperCase()+"::jobs::posted", 0, -1);

                    mult.exec((errMult,resMult)=>{
                        if(errMult) {
                            reject("REDIS ERROR (mult in master's _checkActiveJobLivelyness):"+errMult);
                            return;
                        }

                        // now push back to todo
                        let mult2 = this._redisClient.multi();

                        // remove worker id and push
                        for(let i=0;i<jobsToDrop.length;i++) {
                            let splittedJob = jobsToDrop[i].split("::");
                            // send pub/sub message to kill unresponsive replica
                            this._sendMessage("KILL", splittedJob[2]);
                            // now try to find the posted job to update its timeout
                            for(let j=0;j<resMult[resMult.length-1].length;j++) {
                                let splittedPostedJob = resMult[resMult.length-1][j].split("::");
                                if(splittedJob[0]==splittedPostedJob[1] &&
                                   splittedJob[1]==splittedPostedJob[2] &&
                                   splittedJob[3]==splittedPostedJob[3]) {
                                    // first remove it and repush with updated timestamp
                                    mult2.lrem(this._currency.toUpperCase()+"::jobs::posted", 1, resMult[resMult.length-1][j]);
                                    mult2.lpush(this._currency.toUpperCase()+"::jobs::posted", ""+Date.now()+"::"+splittedJob[0]+"::"+splittedJob[1]+"::"+splittedJob[3]);
                                    // repush it todo
                                    mult2.lpush(""+this._currency.toUpperCase()+"::jobs::todo", ""+splittedJob[0]+"::"+splittedJob[1]+"::"+splittedJob[3]);
                                    break;
                                }
                            }
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
            this._workersDelay = [];
            // send a ping message
            this._sendMessage("CALLROLL", "request");
            this._debug("Sending a callroll as master");
            this._lastCallRollTime = Date.now();
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

    _checkTimedOutJobs() {
        return new Promise((resolve,reject)=>{
            // get the list of done jobs
            // get the list of posted jobs
            let multi1 = this._redisClient.multi();
            multi1.lrange(this._currency.toUpperCase()+"::jobs::done", 0, -1);
            multi1.lrange(this._currency.toUpperCase()+"::jobs::posted", 0, -1);
            multi1.exec((errEx1, doneAndPostedLists)=>{

                if(errEx1) {
                    this._logErrors("A redis request failed.");
                    reject(errEx1);
                    return;
                }

                // do nothing if nothing to check
                if(doneAndPostedLists[1]==null || 
                  (Array.isArray(doneAndPostedLists[1])==false && doneAndPostedLists[1].length==0)) {
                    resolve();
                    return;
                }
                // as a remainer for job formats:
                // in todo list: keyspace::JOB_NAME::firstblock,lastblock
                // in doing and done lists: keyspace::JOB_NAME::workeridentifier::firstblock,lastblock
                // in posted list: timestamp::keyspace::JOB_NAME::firstblock,lastblock

                // save a reference to both lists for easier access
                let postedJobs = doneAndPostedLists[1];
                let doneJobs = [];
                let timedoutJobs = [];
                let toResetJobs = [];
                // our object to bulk redis calls
                let delMulti = this._redisClient.multi();
                if(doneAndPostedLists[0]!=null)doneJobs=doneAndPostedLists[0];
                // for each posted job
                for(let i=0;i<postedJobs.length;i++) {

                    let foundAndDeleted = false;
                    let postedJobSplit = postedJobs[i].split("::");
                    // for each in the done job
                    for(let j=0;j<doneJobs.length;j++) {
                        let doneJobSplit = doneJobs[j].split("::");
                        // if they match
                        if((postedJobSplit[1]==doneJobSplit[0]) 
                        && (postedJobSplit[2]==doneJobSplit[1]) 
                        && (postedJobSplit[3]==doneJobSplit[3])) {
                            // queue a job deletion in both lists
                            delMulti.lrem(this._currency.toUpperCase()+"::jobs::posted",1,postedJobs[i]);
                            delMulti.lrem(this._currency.toUpperCase()+"::jobs::done",1,doneJobs[j]);
                            // dump metrics if asked for 
                            let timeTaken = Date.now() - Number(postedJobSplit[0]);
                            delMulti.lpush(this._currency.toUpperCase()+"::metrics-data::jobTimeout", ""+Date.now()+"::"+postedJobSplit[3]+"::"+timeTaken);
                            foundAndDeleted=true;
                            // break out of the for loop
                            break;
                        }
                    }
                    // if the posted job had no match
                    if(foundAndDeleted==false) {
                        // if the job has timed out
                        if(Math.abs(Number(postedJobSplit[0])-Date.now())>JOB_LIFE_TIMEOUT) {
                            // put it in the timeout list for later processing
                            timedoutJobs.push(postedJobs[i]);
                        }
                    }
                }
                // execute the queued job deletions
                delMulti.exec((errDM, resDM)=>{
                    if(errDM) {
                        reject(errDM);
                        return;
                    }
                    // if timeout list is empty, resolve
                    if(timedoutJobs.length==0) {
                        resolve();
                        return;
                    }
                    // if not, get the todo and doing job lists
                    let listMult = this._redisClient.multi();
                    listMult.lrange(this._currency.toUpperCase()+"::jobs::todo", 0, -1);
                    listMult.lrange(this._currency.toUpperCase()+"::jobs::doing", 0, -1);
                    listMult.exec((errLMul, lists)=>{
                        if(errLMul) {
                            reject(errLMul);
                            return;
                        }
                        // give lists easier names

                        let toClear = [];

                        let todo = [];
                        let doing = [];
                        if(lists[0]!=null)todo=lists[0];
                        if(lists[1]!=null)doing=lists[1];
                        // for each job of the timeout list
                        for(let i=0;i<timedoutJobs.length;i++) {
                            let timedoutJobSplit = timedoutJobs[i].split("::");

                            // for each job in the doing list
                            for(let j=0;j<doing.length;j++) {
                                let doingJobSplit = doing[j].split("::");
                                // if they match, found=true and break
                                if((timedoutJobSplit[1]==doingJobSplit[0]) 
                                && (timedoutJobSplit[2]==doingJobSplit[1]) 
                                && (timedoutJobSplit[3]==doingJobSplit[3])) {
                                    // send a signal to kill its worker 
                                    /* message is formatted as so: "[TIMESTAMP]::[IDENTIFIER_EMITTER]::[ACTION]::[PARAMETER1,PARAMETER2,etc..]*/
                                    // action is KILL, with parameter as the workerId
                                    let workerId = doingJobSplit[2];
                                    // the helper that format the message as above
                                    this._sendMessage("KILL", workerId);
                                    toClear.push({type:"doing",job:doing[j]});
                                    break;
                                }
                            }

                            // for each job in the todo list
                            for(let j=0;j<todo.length;j++) {
                                let todoJobSplit = todo[j].split("::");
                                // if they match and break
                                if((timedoutJobSplit[1]==todoJobSplit[0]) 
                                && (timedoutJobSplit[2]==todoJobSplit[1]) 
                                && (timedoutJobSplit[3]==todoJobSplit[2])) {
                                    toClear.push({type:"todo", job:todo[j]});
                                    break;
                                }
                            }

                            // push this job in the reset list
                            toResetJobs.push(timedoutJobs[i]);
                        }
                        let repostMult = this._redisClient.multi();
                        // for each job in the reset list
                        for(let i=0;i<toResetJobs.length;i++) {
                            let splittedResetJob = toResetJobs[i].split("::");
                            // increment the timedout job count (for this keyspace)
                            repostMult.incr(this._currency.toUpperCase()+"::"+splittedResetJob[1]+"::timedout-jobs");
                            // queue a redis deletion from posted list (we replace it here)
                            repostMult.lrem(this._currency.toUpperCase()+"::jobs::posted", 1, toResetJobs[i]);
                            // queue a redis push in posted with updated timestamp
                            splittedResetJob[0] = String(Date.now());
                            // queue a new job push in the todo list
                            repostMult.lpush(this._currency.toUpperCase()+"::jobs::todo", splittedResetJob[1]+"::"+splittedResetJob[2]+"::"+splittedResetJob[3]);
                            // timeout warning
                            this._logErrors("A job has timed out and has been reset: "+toResetJobs[i]);
                            // report the job with updated timeout
                            repostMult.lpush(this._currency.toUpperCase()+"::jobs::posted", splittedResetJob.join("::"));
                        }
                        // remove old ones from todo and doing
                        for(let i=0;i<toClear.length;i++) {
                            if(toClear[i].type=="todo") {
                                repostMult.lrem(this._currency.toUpperCase()+"::jobs::todo", 1, toClear[i].job);
                            } else if(toClear[i].type=="doing") {
                                repostMult.lrem(this._currency.toUpperCase()+"::jobs::doing", 1, toClear[i].job);
                            }
                        }
                        // execute the final call
                        repostMult.exec((errRP,resRP)=>{
                            if(errRP) {
                                reject(errRP);
                                return;
                            }
                            resolve();
                            return;
                        });
                    });
                });
            });
        });
    }

    _updateMetrics() {
        let metricMult1 = this._redisClient.multi();
        // pop last 100 redis timeouts (shouldn't be more than 6)
        metricMult1.lpop(this._currency.toUpperCase()+"::metrics-data::redis-timeouts", 100);
        // execute
        metricMult1.exec((errMul1, resMul1)=>{
            if(errMul1) {
                this._logErrors(errMul1);
                return;
            }

            // second bulk of redis calls
            let metricMult2 = this._redisClient.multi();
            // set references for easier code reading
            let redisResponseTimes = [];
            if(resMul1[0]!=null) {
                redisResponseTimes = resMul1[0];
            }
            let jobsProcessingTimesLength = resMul1[1];
            // compute average rate of redis timeouts
            let avgRedis = 0;
            for(let i=0;i<redisResponseTimes.length;i++) {
                avgRedis+=Number(redisResponseTimes[i]);
            }
            avgRedis = avgRedis/redisResponseTimes.length;
            metricMult2.set(this._currency.toUpperCase()+"::metrics::redis-timeout", avgRedis);
            this._redisClient.publish(this._currency.toUpperCase()+"::metrics", "redis-timeout: "+avgRedis);

            // get all job processing times
            metricMult2.lrange(this._currency.toUpperCase()+"::metrics-data::jobTimeout", 0, -1);

            metricMult2.exec((errMul2, resMul2)=>{
                if(errMul2) {
                    this._logErrors(errMul2);
                    return;
                }

                // compute average job time and average block time if needed (see last if before mult2 exec)
                if(resMul2[1]!=null && resMul2[1].length!=0) {
                    let avgJobTime=0;
                    let avgBlockTime=0;
                    let blocks=0;
                    for(let i=0;i<resMul2[1].length;i++) {
                        let range = resMul2[1][i].split("::")[1].split(",");
                        blocks+=(Number(range[1])-Number(range[0]));
                        avgJobTime += Number(resMul2[1][i].split("::")[2]);
                    }
                    avgBlockTime = avgJobTime/blocks;
                    avgJobTime = avgJobTime/resMul2[1].length;
                
                    let metricsMult3 = this._redisClient.multi();
                    metricsMult3.publish(this._currency.toUpperCase()+"::metrics", "avg-job-time: "+avgJobTime);
                    metricsMult3.publish(this._currency.toUpperCase()+"::metrics", "avg-block-time: "+avgBlockTime);
                    metricsMult3.set(this._currency.toUpperCase()+"::metrics::avg-job-time", avgJobTime);
                    metricsMult3.set(this._currency.toUpperCase()+"::metrics::avg-block-time", avgBlockTime);
                    metricsMult3.del(this._currency.toUpperCase()+"::metrics-data::jobTimeout");

                    // push the average job time and trigger range adaptation if enought data
                    // 10 is the moving average size used = 5 minutes of master running
                    this._lastBlockSpeeds.push(avgBlockTime);
                    if(this._lastBlockSpeeds.length>0) {
                        if(this._lastBlockSpeeds.length>4) {
                            this._lastBlockSpeeds.shift();
                        }
                        this._scaleBlockRange();
                    }

                    metricsMult3.exec((errMult3,resMult3)=>{
                        if(errMult3) {
                            this._logErrors(errMult3);
                        }
                    });
                } else {
                    return;
                }
            });
        });
    }

    _rangeSizeForHeight(height) {
        if(height<150000) {
            return 800;
        } else if (height<180000) {
            return 500;
        } else if (height<230000) {
            return 200;
        } else if (height<300000) {
            return 100;
        } else if (height<400000) {
            return 50;
        } else {
            return 20;
        }
    }
}

module.exports = {MasterRole};
