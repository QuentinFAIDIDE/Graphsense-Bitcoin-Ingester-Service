const redis = require("redis");
var ExpressCassandra = require("express-cassandra");
const fs = require("fs");
const { exec } = require("child_process");

var MIN_CLIENT_INIT_TIME = 10000;
var MAX_RETRY_CASSANDRA = 50;

var KEYSPACE_REGEXP = /^[A-Za-z0-9_]{1,48}$/;


var address_types = {
    "nonstandard": 1,
    "pubkey": 2,
    "pubkeyhash": 3,
    "multisig_pubkey": 4,
    "scripthash": 5,
    "multisig": 6,
    "nulldata": 7,
    "witness_pubkeyhash": 8,
    "witness_scripthash": 9,
    "witness_unknown": 10
};

// trick for the enum to work on the json-like ETL format
let None = null;

class CassandraWriter {
    /*
    Class that implements writings to cassandra
  */

    constructor(redisHost, redisPort, symbol, logMessage, logErrors, debug) {
        // load a redis instance to increment on the write or save errors
        this._redisClient = redis.createClient({ port: redisPort, host: redisHost });

        this._currency = symbol;
        this._logMessage = logMessage;
        this._logErrors = logErrors;
        this._debug = debug;

        // initalize the map where we will save the cassandra instances
        this._cassandraDrivers = {};
        this._transactionModels = {};
        this._summaryStatisticsModels = {};
        this._blockModels = {};
        this._blockTransactionsModels = {};
        this._exchangeRatesModels= {};
        this._cassandraDriversTimestamps = {};

        // read the schema template synchronously
        // let's not take any risk with race conditions
        this._cqlSchemaTemplate = fs.readFileSync("./scripts/schema.cql");

        // options to provide to new clients for each keyspace
        this._clientOptions = {
            contactPoints: process.env.CASSANDRA_CONTACT_POINTS.split(","),
            protocolOptions: {port: process.env.CASSANDRA_PORT},
            keyspace: "mykeyspace",
            localDataCenter: process.env.CASSANDRA_DATACENTER,
            queryOptions: {consistency: ExpressCassandra.consistencies.one},
            socketOptions: {
                connectTimeout: 15000
            }
        };
        this._ormOptions = {
            defaultReplicationStrategy:
            {class: "SimpleStrategy", replication_factor: 2},
            migration: "safe"
        };

        // transaction model
        this._transactionModel = {
            fields: {
                tx_prefix: "text",
                tx_hash: "blob",
                tx_index: "bigint",
                height: "int",
                timestamp: "int",
                coinbase: "boolean",
                total_input: "bigint",
                total_output: "bigint",
                inputs: {
                    type: "list",
                    typeDef: "<FROZEN<tx_input_output>>"
                },
                outputs: {
                    type: "list",
                    typeDef: "<FROZEN<tx_input_output>>"
                },
                coinjoin: "boolean"
            },
            key: ["tx_prefix", "tx_hash"]
        };

        // summary statistics model
        this._summaryStatisticsModel = {
            fields: {
                id: "text",
                no_blocks: "int",
                no_txs: "bigint",
                timestamp: "int"
            },
            key: ["id"]
        };

        // block model
        this._blockModel = {
            fields: {
                height: "int",
                block_hash: "blob",
                no_transactions: "int",
                timestamp: "int"
            },
            key: ["height"]
        };

        this._exchangeRatesModel = {
            fields: {
                date: "text",
                eur: "float",
                usd: "float"
            },
            key: ["date"]
        };

        // block_transactions model
        this._blockTransactionsModel = {
            fields: {
                height: "int",
                txs: {
                    type: "list",
                    typeDef: "<FROZEN<tx_summary>>"
                }
            },
            key: ["height"]
        };

        this._csvHeaderPrefix = "hash,size,stripped_size";

        this._jobErrors = {};
        this._totalBlocksPerJob = {};
        this._writtenBlocksPerJob = {};
        this._receivedBlocksPerJob = [];
        this._blockTransactionMaps = {};
        this._garbageCollection = {};
        this._jobWriteRetryStack = {};
        this._jobDoneCallbacks = {};
        this._jobTxCount = {};
    }

    prepareForKeyspace(keyspace) {
        return new Promise((resolve,reject)=>{
            if(KEYSPACE_REGEXP.test(keyspace)==false) {
                reject("Invalid keyspace name.");
                return;
            }
            try {
                // test if we don't already have a client for this keyspace
                if(typeof this._cassandraDrivers[keyspace] == "undefined") {
                    // write the file with the schema definition
                    fs.writeFile("scripts/"+keyspace+".cql", 
                        String(this._cqlSchemaTemplate).replace(/\[MY_KEYSPACE_NAME\]/g, keyspace), (err)=>{
                        // check for write errors
                            if(err) {
                                reject("Error while writing keyspace schema file:"+err);
                                return;
                            }
                        
                            // exec the cql command to create keyspace
                            exec("cqlsh "+process.env.CASSANDRA_CONTACT_POINTS.split(",")[0]+
                        " "+process.env.CASSANDRA_PORT+" < scripts/"+keyspace+".cql",
                            (errCQLSH, stoCQLSH, steCQLSH) => {

                            // check for errors
                                if(errCQLSH) {
                                    reject("Error while creating cassandra schema:"+errCQLSH);
                                    return;
                                }

                                // if not, create it and save the date
                                this._cassandraDriversTimestamps[keyspace] = Date.now();
                                // customize params for our keyspace
                                let clientOptions =  this._clientOptions;
                                clientOptions.keyspace = keyspace;
                                // now it's time to start it
                                this._cassandraDrivers[keyspace] = ExpressCassandra.createClient(
                                    {
                                        clientOptions: clientOptions,
                                        ormOptions: this._ormOptions
                                    });
                                // create models
                                this._transactionModels[keyspace] = this._cassandraDrivers[keyspace].loadSchema("transaction", this._transactionModel);
                                this._blockModels[keyspace] = this._cassandraDrivers[keyspace].loadSchema("block", this._blockModel);
                                this._blockTransactionsModels[keyspace] = this._cassandraDrivers[keyspace].loadSchema("block_transactions", this._blockTransactionsModel);
                                this._summaryStatisticsModels[keyspace] = this._cassandraDrivers[keyspace].loadSchema("summary_statistics", this._summaryStatisticsModel);
                                this._exchangeRatesModels[keyspace] = this._cassandraDrivers[keyspace].loadSchema("exchange_rates", this._exchangeRatesModel);
                                // wait and start
                                setTimeout(resolve, MIN_CLIENT_INIT_TIME-Math.abs(this._cassandraDriversTimestamps[keyspace]-Date.now()));

                            });
                        });
                } else {
                    // if it's old enought
                    if(Math.abs(this._cassandraDriversTimestamps[keyspace]-Date.now())>MIN_CLIENT_INIT_TIME) {
                        // resolve immediately
                        resolve();
                    // but if it's too young to work (cassandra is too cool to start in less than 20seconds on some keyspace configs)
                    // reminder: writing to an uninitialized client can trigger the deadly timeout error
                    } else {
                        // wait a bit before resolving
                        setTimeout(resolve, MIN_CLIENT_INIT_TIME-Math.abs(this._cassandraDriversTimestamps[keyspace]-Date.now()));
                    }
                }
            } catch(err) { reject(err); }
        });
    }

    registerJob(jobname, callback) {
        // create the error list to store job errors
        this._jobErrors[jobname] = [];
        // now, create the job map for block
        this._blockTransactionMaps[jobname] = {};
        // save the total number of block to write
        // we will use it clear the blocktransaction map after writes
        let range = jobname.split("::")[3].split(",");
        this._totalBlocksPerJob[jobname] = (range[1]-range[0])+1;
        this._receivedBlocksPerJob[jobname] = 0;
        this._writtenBlocksPerJob[jobname] = 0;
        // garbage collection for transactions
        this._garbageCollection[jobname] = [];
        this._jobWriteRetryStack[jobname] = {
            "transaction": [],
            "block": [],
            "block_transactions": [],
            "count": 0
        };
        // callback fired once transaction have been recovered
        this._jobDoneCallbacks[jobname] = callback;
        // count to know total number of tx to write
        this._jobTxCount[jobname] = {
            "blocks_finished": false,
            "txToWrite": 0,
            "txReceived": 0
        };
    }

    getJobStatus(jobname) {
        return new Promise((resolve, reject)=>{
            // This function should only be called after the job callback was triggered

            // report on errors for this job and clear the error list
            let errmsg;
            // returns null after clearing errors object for this jobname if no error
            if(this._jobErrors[jobname].length==0) {
                this._clearJob(jobname);
                resolve(null);
            } else {
            // if error, return a custom error message with number of error and first one
                errmsg = "There were "+this._jobErrors[jobname].length+" errors, first one being: "+this._jobErrors[jobname][0];
                this._clearJob(jobname);
                resolve(errmsg);
            }
        });
    }

    _clearJob(jobname) {
        // clear all auxilary objects for this job
        delete this._blockTransactionMaps[jobname];
        delete this._writtenBlocksPerJob[jobname];
        delete this._totalBlocksPerJob[jobname];
        delete this._garbageCollection[jobname];
        delete this._jobWriteRetryStack[jobname];
        delete this._jobDoneCallbacks[jobname];
        delete this._jobTxCount[jobname];
        delete this._receivedBlocksPerJob[jobname];
        delete this._jobErrors[jobname];
    }

    parseBlock(keyspace, jobname, blockbuffer) {
        // header: 
        // hash,size,stripped_size,weight,number,version,merkle_root,timestamp,nonce,bits,coinbase_param,transaction_count
        
        // parse lines
        let lines = String(blockbuffer).split("\n");

        // create request object to bulk writes
        let queries = [];

        // buffer to parse blocks
        let blockObj = {};

        // for each line
        for(let i=0;i<lines.length;i++) {
            // ignore empty lines
            if(lines[i].length!=0) {
                try {
                    blockObj = JSON.parse(lines[i]);
                } catch(err) {
                    this._logErrors("Corrupted block received:"+lines[i]);
                    continue;
                }
                // save the block number of transacts 
                // also add an entry for the transaction map if needed (means no tx arrived yet)
                if(this._blockTransactionMaps[jobname].hasOwnProperty(blockObj.number)==true) {
                    // if it exists and we received it, he total_tx has not been possibly set already
                    // we will check in case it's a retry to avoid incrementing txToWrite count unecessarely
                    if(typeof this._blockTransactionMaps[jobname][blockObj.number].total_tx == "undefined") {
                        // this is used to aggregate block_transactions table rows
                        this._blockTransactionMaps[jobname][blockObj.number].total_tx = blockObj.transaction_count;
                        // this is used to know when all tx have been written, failed or not
                        this._jobTxCount[jobname].txToWrite += blockObj.transaction_count;
                    }
                } else {
                    // create the map to store transactions infos in their blocks
                    this._blockTransactionMaps[jobname][blockObj.number] = {
                        total_tx: blockObj.transaction_count,
                        writen_tx: 0,
                        tx_summary_list: []
                    };
                    this._jobTxCount[jobname].txToWrite += blockObj.transaction_count;
                    this._receivedBlocksPerJob[jobname]++;
                    if(this._receivedBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                        this._jobTxCount[jobname].blocks_finished = true;
                    }
                }
                // create model
                queries.push( new this._blockModels[keyspace]({
                    height: blockObj.number,
                    block_hash: Buffer.from(blockObj.hash, "hex"),
                    no_transactions: blockObj.transaction_count,
                    timestamp: blockObj.timestamp
                }).save({return_query: true}));
            }
        }

        // send the writes
        this._cassandraDrivers[keyspace].doBatch(queries, (err)=>{
            if(err)this._manageWriteErrorsForJob(jobname, err, queries, "block");
        });
    }

    // function to format data for the block_transactions table
    _addTransactionToBlockSummary(keyspace, jobname, height, hash, nin, nout, tin, tout) {
        // if the block is in the map
        if(this._blockTransactionMaps[jobname].hasOwnProperty(height)==true) {
            // push the new tx summary
            this._blockTransactionMaps[jobname][height].tx_summary_list.push({
                tx_hash: hash,
                no_inputs: nin,
                no_outputs: nout,
                total_input: tin,
                total_output: tout
            });
            // increment tx processed counter
            this._blockTransactionMaps[jobname][height].writen_tx++;
            // if we have finished the block, send it for a write
            if(this._blockTransactionMaps[jobname][height].writen_tx>=this._blockTransactionMaps[jobname][height].total_tx) {
                this._writeBlockTransactionSummary(keyspace, jobname, height);
            }
        } else {
            // push an error
            this._jobErrors[jobname].push("FATAL ERROR: A transaction was received before its block.");
        }
    }

    _writeBlockTransactionSummary(keyspace, jobname, height) {
        // create the row with all the txs
        let row = new this._blockTransactionsModels[keyspace]({
            height: Number(height),
            txs: this._blockTransactionMaps[jobname][height].tx_summary_list
        });
        // now write the row
        row.save((err)=>{
            if(err) {
                this._manageWriteErrorsForJob(jobname, err, [height], "block_transactions");
                return;
            }
            // increment block summary written
            this._writtenBlocksPerJob[jobname]++;

            // if last block
            if(this._writtenBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                this._debug("Job "+jobname+" received all transactions and blocks...");
                // now if all transaction were received, execute the cleanup
                if(this._jobTxCount[jobname].txReceived>=this._jobTxCount[jobname].txToWrite) {
                    // if all blocks have been received and filled with txs
                    if(this._jobTxCount[jobname].blocks_finished==true) {
                        this._startRecoveringWriteErrors(keyspace, jobname);
                    // in case of write finishing before tx aggreg in block_transactions
                    } else {
                        this._jobErrors.push("FATAL ERROR: transactions were all written before blocks were all received");
                    }
                }
            } else {
                // if not the last block, clear at least this block txs
                // we want to avoid blowing up nodejs limited memory stack to maintain descent replicas error rates
                if(this._blockTransactionsModels.hasOwnProperty(jobname) && 
                   this._blockTransactionsModels[jobname].hasOwnProperty(height)) {
                    delete this._blockTransactionMaps[jobname][height];
                }
            }
        });
    }

    _clearAndTerminate(jobname) {
        // call the callback for when everything's done
        this._jobDoneCallbacks[jobname]();
        // stream updates to cli clients
        this._redisClient.publish("BTC::done", jobname);
    }

    _manageWriteErrorsForJob(jobname, err, rows=null, table=null) {
        // in case of error, save it for the worker service to retrieev it later
        if(err) {
            this._logErrors("Cassandra error at write:"+err);
            // if the error happens after the pipe have been cleared (shouldn't happen anymore)
            if(typeof this._jobErrors[jobname] == "undefined") {
                // activate emergency procedure to remove job from done stack and push it to errors
                this._moveJobFromDoneToErrorStack(jobname);
                // tell the cli clients about it (if they are listening)
                this._redisClient.publish(""+jobname+"::errors",  "job failed to execute: "+jobname);
                return;
            }
            //if the job is still running save the error if we didn't saved too much already
            if(this._jobWriteRetryStack[jobname].count<MAX_RETRY_CASSANDRA) {
                if(rows!=null) {
                    for(let i=0;i<rows.length;i++) {
                        this._jobWriteRetryStack[jobname][table].push(rows[i]);
                        this._jobWriteRetryStack[jobname].count++;
                    }
                }
            // if the job has reached maximum allowed write errors, push an error to the stack
            } else {
                this._jobErrors[jobname].push("FATAL ERROR: cassandra error rate is too high, error recovery stack max size has been reached.");
            }
        }
    }

    parseTransaction(keyspace, jobname, txbuffer, garbageCollection=false) {

        // transaction model (bitcoin-etl)
        // yes - no -    no      -   no    -  no    -   yes     -    no      -    yes     -       yes  - no  -   yes - yes -     yes    -       yes        yes          yes      no                          
        // hash,size,virtual_size,version,lock_time,block_number,block_hash,block_timestamp,is_coinbase,index,inputs,outputs,input_count,output_count,input_value,output_value,fee

        // transaction model (graphsense)
        // this._transactionModel = {
        //     fields: {
        //         tx_prefix: "text",
        //         tx_hash: "blob",
        //         tx_index: "bigint",
        //         height: "int",
        //         timestamp: "int",
        //         coinbase: "boolean",
        //         total_input: "bigint",
        //         total_output: "bigint",
        //         inputs: {
        //             type: "list",
        //             typeDef: "<FROZEN<tx_input_output>>"
        //         },
        //         outputs: {
        //             type: "list",
        //             typeDef: "<FROZEN<tx_input_output>>"
        //         },
        //         coinjoin: "boolean"
        //     },
        //     key: ["tx_prefix", "tx_hash"]
        // };

        // parse lines
        let lines = String(txbuffer).split("\n");

        // create request object to bulk writes
        let queries = [];
        let row;
        let jsonObj;

        // for each line
        for(let i=0;i<lines.length;i++) {

            // ignore headers
            if(lines[i]!="") {
                try  {
                    jsonObj = JSON.parse(lines[i]);
                    row = {
                        tx_prefix: jsonObj.hash.substring(0,5),
                        tx_hash: Buffer.from(jsonObj.hash,"hex"),
                        tx_index: null,
                        height: jsonObj.block_number,
                        timestamp: jsonObj.block_timestamp,
                        coinbase: jsonObj.coinbase,
                        total_input: ExpressCassandra.datatypes.Long.fromInt(jsonObj.input_value,10),
                        total_output: ExpressCassandra.datatypes.Long.fromInt(jsonObj.output_value,10),
                        inputs: this._inputConvertETLtoGraphSense(jsonObj.inputs),
                        outputs: this._inputConvertETLtoGraphSense(jsonObj.outputs),
                        coinjoin: false
                    };

                    // detect coinjoin
                    row.coinjoin = this._detectCoinjoin(row);

                    // save the tx summary to our maps
                    this._addTransactionToBlockSummary(keyspace, jobname, row.height, row.tx_hash,
                        row.inputs.length, row.outputs.length, row.total_input,
                        row.total_output);

                    // initalize cassandra row and prepare it to be sent with others
                    queries.push( new this._transactionModels[keyspace](row).save({return_query: true}));

                    // if the transaction was a garbage collection, clean the garbage
                    if(garbageCollection==true) {
                        this._debug("Garbage collection success with size:"+this._garbageCollection[jobname].length);
                        this._garbageCollection[jobname] = [];
                    }

                    if(this._garbageCollection[jobname].length!=0) {
                        this._debug("A transaction worked despise the garbage collection list not being empty!!");
                    }
                } catch(err) {
                    // this is a tricky part, our pipe sometimes break down lines
                    // we must collect the garbage and try to glue it into a valid json

                    // if this tx data is a garbage collection, ignore the error
                    if(garbageCollection==true) {
                        this._debug("Garbage collection failed with size "+this._garbageCollection[jobname].length+", saving garbage for later.");
                    } else {

                        this._debug("Approximate Tx Buffer size that failed to parse (bytes): "+ String(txbuffer).length*2);
                        // if the json was cut down, save the part that is broken
                        if(String(err).indexOf("Unexpected end of JSON input")!=-1 || 
                           String(err).indexOf("in JSON at position")!=-1) {
                            this._garbageCollection[jobname].push(lines[i]);
                            // if we have garbage saved
                            if(this._garbageCollection[jobname].length>1) {
                                // try to glue it all together and send it again
                                this.parseTransaction(keyspace,jobname, this._garbageCollection[jobname].join("").replace(/\n/g, ""), true);
                            }
                        // if it's another issue, throw an error
                        } else {
                            throw err;
                        }
                    }
                }
            }
        }

        // send the writes
        this._cassandraDrivers[keyspace].doBatch(queries, (err)=>{
            if(err)this._manageWriteErrorsForJob(jobname, err, queries, "transaction");
            // failure or not, we need to know when all tx have been received to start recovering
            // hence the count and test
            this._jobTxCount[jobname].txReceived+=queries.length;
            if(this._jobTxCount[jobname].txReceived>=this._jobTxCount[jobname].txToWrite) {
                // if all blocks have been received and filled with txs
                if(this._jobTxCount[jobname].blocks_finished==true) {
                    // if all block_transaction rows were written
                    if(this._writtenBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                        this._startRecoveringWriteErrors(keyspace, jobname);
                    }
                // in case of write finishing before tx aggreg in block_transactions
                } else {
                    this._jobErrors.push("FATAL ERROR: transactions were all written before blocks were all received");
                }
            }
        });
    }

    _startRecoveringWriteErrors(keyspace, jobname) {

        this._debug("Started error recovery routine...");

        let recoveredCount = 0;
        let failedCount = 0;
        let totalToRecover = this._jobWriteRetryStack[jobname].count;

        // if there are not errors to recover, do nufin
        if(totalToRecover==0) {
            this._debug("No error to recover, cleaning up job auxiliary objects...");
            this._clearAndTerminate(jobname);
            return;
        }

        // callback to register successes and failures and detect the end
        let recoveredCallback = (success)=>{
            if(success==true) {
                recoveredCount++;
            } else {
                failedCount++;
            }
            // if the three bulk calls are done
            if((recoveredCount+failedCount)>=3) {
                // if we were unable to do the failed write again
                // push a fatal job error
                if(failedCount!=0) {
                    this._jobErrors[jobname].push("FATAL ERROR: job had "+
                  totalToRecover+" failed cassandra writes and could not recover some."+
                  " Watch your cassandra error rate.");
                }
                // fire the end of job callback, the calling service 
                // must now get the job status to know about errors
                this._clearAndTerminate(jobname);
                return;
            }
        };

        // send block_transactions for rewrite
        let blockTransacQueries = [];
        for(let i=0;i<this._jobWriteRetryStack[jobname]["block_transactions"].length;i++) {
            // used to bulk calls
            let height = this._jobWriteRetryStack[jobname]["block_transactions"][i];
            // push it to the list of calls to make
            blockTransacQueries.push(new this._blockTransactionsModels[keyspace]({
                height: height,
                txs: this._blockTransactionMaps[jobname][height].tx_summary_list
            }).save({return_query: true}));
        }
        // send all the block transac
        this._cassandraDrivers[keyspace].doBatch(blockTransacQueries, (err)=>{
            if(err) {
                this._logErrors(err);
                recoveredCallback(false);
            } else {
                recoveredCallback(true);
            }
        });

        // send block for rewrite
        let blockQueries = [];
        for(let i=0;i<this._jobWriteRetryStack[jobname]["block"].length;i++) {
            // used to bulk calls
            let row = this._jobWriteRetryStack[jobname]["block"][i];
            // push it to the list of calls to make
            blockQueries.push(new this._blockModels[keyspace](row).save({return_query: true}));
        }
        // send all the block transac
        this._cassandraDrivers[keyspace].doBatch(blockQueries, (err)=>{
            if(err) {
                this._logErrors(err);
                recoveredCallback(false);
            } else {
                recoveredCallback(true);
            }
        });

        // send block for rewrite
        let txQueries = [];
        for(let i=0;i<this._jobWriteRetryStack[jobname]["transaction"].length;i++) {
            // used to bulk calls
            let row = this._jobWriteRetryStack[jobname]["transaction"][i];
            // push it to the list of calls to make
            txQueries.push(new this._transactionModels[keyspace](row).save({return_query: true}));
        }
        // send all the block transac
        this._cassandraDrivers[keyspace].doBatch(txQueries, (err)=>{
            if(err) {
                this._logErrors(err);
                recoveredCallback(false);
            } else {
                recoveredCallback(true);
            }
        });
    }

    _inputConvertETLtoGraphSense(etlObj) {

        if(Array.isArray(etlObj)==false) {
            // something's wrong, corrupted data
            this._logErrors("Corrupted transaction input/output received: "+JSON.stringify(etlObj));
            return [];
        }

        // let's now convert the data
        let graphsenseObj = [];
        for(let i=0;i<etlObj.length;i++) {
            let adtypeid = 0;
            if(address_types.hasOwnProperty(etlObj[i].type)==true) {
                adtypeid = address_types[etlObj[i].type];
            }
            graphsenseObj.push({
                address: etlObj[i].addresses,
                value: etlObj[i].value,
                address_type: adtypeid
            });
        }

        return graphsenseObj;
    }

    _moveJobFromDoneToErrorStack(jobname) {
        // remove the job from the doing list
        this._redisClient.lrem(""+this._currency+"::jobs::done", 1, jobname, (errLREM, resLREM)=>{
            if(errLREM) {
                this._logErrors("Lost a job we couldn't remove from done list: "+ jobname);
                return;
            }

            // check if we indeed removed something
            // if not throw an error message
            if(resLREM==0) {
                this._logErrors("Job was not found in the done list (while trying to move it to error list)!");
                return;
            }

            // push it to error list
            this._redisClient.lpush(""+this._currency+"::jobs::errors", jobname, (errLP, resLP)=>{
                if(errLP) {
                    this._logErrors("Lost a job we couldn't push to error list:"+jobname);
                    return;
                }
            });
        });
    }

    _detectCoinjoin(tx) {
        // we will reproduce Blocksci's algo from:
        // https://github.com/citp/BlockSci/blob/master/src/heuristics/tx_identification.cpp
        // warning: it's more of a wild guess than a decisive coinjoin detection

        if (tx.inputs.length < 2 || tx.outputs.length < 3) {
            return false;
        }

        // Each participant contributes a spend and a change output
        let participantCount = (tx.outputs.length + 1) / 2;
        if (participantCount > tx.inputs.length) {
            return false;
        }

        // return false if there are less inputs address that participants
        let input_addrs = [];
        for(let i=0;i<tx.inputs.length;i++) {
            // let's support multisig that have many addrs in same input/output
            try {
                for(let j=0;j<tx.inputs[i].address.length;j++) {
                    if(input_addrs.includes(tx.inputs[i].address[j])==false)
                        input_addrs.push(tx.inputs[i].address[j]);
                }
            } catch(err) {
                this._debug("Faulty input at index "+i+": "+JSON.stringify(tx));
            }
        }
        if(participantCount > input_addrs.length) {
            return false;
        }

        // count occurence of output values
        let output_values_map = [];
        let maxoccur = 0;
        let maxoccured_value = 0;
        for(let i=0;i<tx.outputs.length;i++) {
            if(output_values_map.hasOwnProperty(tx.outputs[i].value)==false) {
                output_values_map[tx.outputs[i].value] = 1;
            } else {
                output_values_map[tx.outputs[i].value]++;
            }
            if(output_values_map[tx.outputs[i].value]>maxoccur) {
                maxoccur = output_values_map[tx.outputs[i].value];
                maxoccured_value = tx.outputs[i].value;
            }
        }
        // the most common output value should appear participantCount times
        if(maxoccur!=participantCount) {
            return false;
        }

        // exclude transactions sending dust
        if(maxoccured_value == 546 || maxoccured_value == 2730) {
            return false;
        }

        return true;
    }

    clearJobIfExists(jobname) {
        // clear all auxilary objects for this job
        if (this._blockTransactionMaps.hasOwnProperty(jobname) == true)
            delete this._blockTransactionMaps[jobname];
        if (this._writtenBlocksPerJob.hasOwnProperty(jobname) == true)
            delete this._writtenBlocksPerJob[jobname];
        if (this._totalBlocksPerJob.hasOwnProperty(jobname) == true)
            delete this._totalBlocksPerJob[jobname];
        if (this._garbageCollection.hasOwnProperty(jobname) == true)
            delete this._garbageCollection[jobname];
        if (this._jobWriteRetryStack.hasOwnProperty(jobname) == true)
            delete this._jobWriteRetryStack[jobname];
        if (this._jobDoneCallbacks.hasOwnProperty(jobname) == true)
            delete this._jobDoneCallbacks[jobname];
        if (this._jobTxCount.hasOwnProperty(jobname) == true)
            delete this._jobTxCount[jobname];
        if (this._receivedBlocksPerJob.hasOwnProperty(jobname) == true)
            delete this._receivedBlocksPerJob[jobname];
        if (this._jobErrors.hasOwnProperty(jobname) == true)
            delete this._jobErrors[jobname];
    }

    writeExchangeRates(keyspace, rates) {
        return new Promise((resolve, reject)=>{
            // where to store rows before pushing
            let queries = [];

            // for each row
            for(let i=0;i<rates.length;i++) {
                // push it for write
                queries.push(new this._exchangeRatesModels[keyspace](rates[i]).save({return_query: true}));
            }

            // execute the bulk write
            this._cassandraDrivers[keyspace].doBatch(queries, (err)=>{
                if(err) {
                    this._logErrors(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

}



module.exports = {CassandraWriter};
