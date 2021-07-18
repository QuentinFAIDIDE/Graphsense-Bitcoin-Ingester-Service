// keeps buggy eslint from throwing errors (unable to set es2020 for whatever reason)
/* global BigInt */

const redis = require("redis");
var ExpressCassandra = require("express-cassandra");
const fs = require("fs");
const { exec } = require("child_process");
const cassandra = require("cassandra-driver");

var MIN_CLIENT_INIT_TIME = 10000;
var MAX_RETRY_CASSANDRA = 50;

var push_transaction_query = "INSERT INTO transaction (tx_prefix, tx_hash, tx_index, height, timestamp, coinbase, total_input, total_output, inputs, outputs, coinjoin) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
var update_tx_inputs_query = "UPDATE transaction SET total_input=?, inputs=?, coinjoin=? WHERE tx_prefix=? AND tx_hash=?";
var write_stats_query = "INSERT INTO summary_statistics (id, no_blocks, no_txs, timestamp) VALUES (?, ?, ?, ?)";

var KEYSPACE_REGEXP = /^[a-z0-9_]{1,48}$/;

var DUMP_METRICS = process.env.DUMP_METRICS;
if(DUMP_METRICS!="true")DUMP_METRICS="false"; 

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
        this._expressCassandraDrivers = {};
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
                connectTimeout: 60000,
                readTimeout: 120000
            },
            encoding: {
                useBigIntAsLong: true
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

        this._jobErrors = {};
        this._totalBlocksPerJob = {};
        this._writtenBlocksPerJob = {};
        this._receivedBlocksPerJob = [];
        this._blockTransactionMaps = {};
        this._garbageCollection = {};
        this._jobCassandraIORetryStack = {};
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
                if(typeof this._expressCassandraDrivers[keyspace] == "undefined") {
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

                                this._debug("Cassandra keyspace creation stderr:"+steCQLSH);
                                this._debug("Cassandra keyspace creation stdout:"+stoCQLSH);

                                // if not, create it and save the date
                                this._cassandraDriversTimestamps[keyspace] = Date.now();
                                // customize params for our keyspace
                                let clientOptions =  this._clientOptions;
                                clientOptions.keyspace = keyspace;
                                // now it's time to start it
                                this._expressCassandraDrivers[keyspace] = ExpressCassandra.createClient(
                                    {
                                        clientOptions: clientOptions,
                                        ormOptions: this._ormOptions
                                    });
                                // create the vanilla driver client as well
                                this._cassandraDrivers[keyspace] = new cassandra.Client(clientOptions);
                                // create models
                                this._transactionModels[keyspace] = this._expressCassandraDrivers[keyspace].loadSchema("transaction", this._transactionModel);
                                this._blockModels[keyspace] = this._expressCassandraDrivers[keyspace].loadSchema("block", this._blockModel);
                                this._blockTransactionsModels[keyspace] = this._expressCassandraDrivers[keyspace].loadSchema("block_transactions", this._blockTransactionsModel);
                                this._summaryStatisticsModels[keyspace] = this._expressCassandraDrivers[keyspace].loadSchema("summary_statistics", this._summaryStatisticsModel);
                                this._exchangeRatesModels[keyspace] = this._expressCassandraDrivers[keyspace].loadSchema("exchange_rates", this._exchangeRatesModel);
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

    registerFillingJob(jobname, callback) {
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
        this._jobCassandraIORetryStack[jobname] = {
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

    registerEnrichingJob(jobname, callback) {
        // create the error list to store job errors
        this._jobErrors[jobname] = [];

        // get the range of blocks the job will need to cover
        let range = jobname.split("::")[3].split(",");

        this._totalBlocksPerJob[jobname] = (range[1]-range[0])+1;
        this._writtenBlocksPerJob[jobname] = 0;

        // in case we get a few cassandra errors
        this._jobCassandraIORetryStack[jobname] = {
            "input-reads": [],
            "input-write": [],
            "count": 0
        };

        // register end of job callback 
        this._jobDoneCallbacks[jobname] = callback;

        // get the keyspace
        let keyspace = jobname.split("::")[0];

        // start iterating over blocks for enrichment
        this._iterateOverEnrichingBlocks(keyspace, jobname, Number(range[0]));
    }

    // TODO OPTIMIZATION: Launch block enrichment in parallell (todo after redis utxo cache setup)
    _iterateOverEnrichingBlocks(keyspace, jobname, firstblock, i=0) {
        this._debug("Iteration over block "+(Number(firstblock)+i));
        let hasStoppedDueToError = false;
        // launch the redis call to get all tx for this block
        let multi = this._redisClient.multi();
        multi.lrange(this._currency.toUpperCase()+"::"+keyspace+"::btxs::"+(Number(firstblock)+i), 0, -1);
        multi.del(this._currency.toUpperCase()+"::"+keyspace+"::btxs::"+(Number(firstblock)+i));
        multi.exec((errMul,resMul)=>{
            if(errMul) {
                this._logErrors("REDIS ERROR (_iterateOverEnrichingBlocks): "+errMul);
                // if we activate the parallell enrichment later, we will need to
                // watch for race condition on the error callback and clearing here
                this._jobDoneCallbacks[jobname](errMul);
                this._clearEnrichingJob(jobname);
                return;
            }

            // we have the list of tx hash to find inputs for in the first resMul array member
            
            // if we have no tx, just skip to next block
            if(resMul[0]==null || (Array.isArray(resMul[0]) && resMul[0].length==0)) {
                this._debug("Block "+String(Number(firstblock)+i)+" was empty for job:"+jobname);
                // if we are done
                if((i+1)>=this._totalBlocksPerJob[jobname]) {
                    // we can recover errors and terminate
                    this._recoverEnrichJobErrors(keyspace, jobname);
                    return;
                // if blocks are remaining, go to next one
                } else {
                    this._iterateOverEnrichingBlocks(keyspace, jobname, firstblock, i+1);
                    return;
                }
            }

            let txToWrite = resMul[0].length;
            let txProcessed = 0;

            // calback when a transaction is done
            // success parameter is not used here but is necessary for the _findTxInputs function
            let txDoneCallback = (success)=>{
                txProcessed++;
                if(txProcessed>=txToWrite) {
                    // if we wrote all blocks
                    if((i+1)>=this._totalBlocksPerJob[jobname]) {
                        // we can recover errors and terminate
                        this._recoverEnrichJobErrors(keyspace, jobname);
                        return;
                    // if blocks are remaining, do em too
                    } else {
                        this._iterateOverEnrichingBlocks(keyspace, jobname, firstblock, i+1);
                        return;
                    }
                }
            };

            // for each transaction
            for(let k=0;k<resMul[0].length;k++) {
                // if an error has been thrown inside a child callback try to stop the operation
                // (may be useless because the for loop will have likely finished  already 
                // when the cassandra read callback will return)
                if(hasStoppedDueToError==true)return;
                let inputData;
                try {
                    // parse the tx inputs data
                    inputData = JSON.parse(resMul[0][k]);
                } catch(err) {
                    this._logErrors("REDIS ERROR (_iterateOverEnrichingBlocks): corrupted tx input data in redis: "+err);
                    // if we activate the parallell enrichment later, we will need to
                    // watch for race condition on the error callback and clearing here
                    hasStoppedDueToError=true;
                    this._jobDoneCallbacks[jobname](err);
                    this._clearEnrichingJob(jobname);
                    return;
                }

                this._findTxInputs(keyspace, jobname, inputData, txDoneCallback);
            }
        });
    }

    _findTxInputs (keyspace, jobname, inputData, txDoneCallback, retry=false) {
        let txFound = 0;
        let txInputs = [];
        let failedCassandraRead = false;
        // input found callback
        let foundInputCallback = ()=>{
            txFound++;
            // if we found all inputs
            if(txFound>=inputData.t.length) {
                // if finding input failed
                if(failedCassandraRead == true) {
                    // save the task data to try to recover at the end
                    if(retry==false)this._manageCassandraErrorsForJob(jobname, new Error("Cassandra: Unable to get inputs for tx"), inputData, "inputs-reads");
                    // increase tx count
                    txDoneCallback(false);
                } else {
                    let total_input = BigInt(0);
                    // compute total input value
                    for(let i=0;i<txInputs.length;i++) {
                        total_input += txInputs[i].value;
                    }
                    // now, run the coinjoin algorithm on the in/outs
                    let coinjoin = this._detectCoinjoin({inputs: txInputs, outputs: inputData.o});
                    // and finally write the transaction
                    // "UPDATE transaction SET total_input=?, inputs=?, coinjoin=? WHERE tx_prefix=? AND tx_hash=?"
                    let params = [ total_input, txInputs, coinjoin, Buffer(inputData.h).toString("hex").substring(0,5), Buffer(inputData.h)];
                    this._cassandraDrivers[keyspace].execute(update_tx_inputs_query, params, { prepare: true })
                        .then(()=>{
                            // resolve call
                            txDoneCallback(true);
                        }).catch((err)=>{
                            if(retry==false) {
                                this._manageCassandraErrorsForJob(jobname, err, params, "input-write");
                            }
                            // resolve call and tell about failure
                            txDoneCallback(false);
                        });
                }
            }
        };

        // for each input in the tx
        for(let j=0;j<inputData.t.length;j++) {
            // TODO OPTIMIZATION: try using redis cache here first
            // get corresponding UTXOs from cassandra
            this._transactionModels[keyspace].findOne({
                tx_prefix: inputData.t[j][0].substring(0,5),
                tx_hash: Buffer.from(inputData.t[j][0], "hex")
            }, { select: ["tx_prefix","tx_hash","outputs"]}, (err, transac)=>{
                // if error, stop right away and push 
                if(err) {
                    failedCassandraRead=true;
                } else {
                    // TODO: find out why request return null (failed write for filling ??)
                    // if no error, get the right output and set it as the input
                    txInputs[j] = transac.outputs[inputData.t[j][1]];
                }
                foundInputCallback();
            });
        }
    }

    getFillingJobStatus(jobname) {
        return new Promise((resolve, reject)=>{
            // This function should only be called after the job callback was triggered

            // report on errors for this job and clear the error list
            let errmsg;
            // returns null after clearing errors object for this jobname if no error
            if(this._jobErrors[jobname].length==0) {
                this._clearFillingJob(jobname);
                resolve(null);
            } else {
            // if error, return a custom error message with number of error and first one
                errmsg = "There were "+this._jobErrors[jobname].length+" errors, first one being: "+this._jobErrors[jobname][0];
                this._clearFillingJob(jobname);
                resolve(errmsg);
            }
        });
    }

    _clearFillingJob(jobname) {
        // clear all auxilary objects for this job
        delete this._blockTransactionMaps[jobname];
        delete this._writtenBlocksPerJob[jobname];
        delete this._totalBlocksPerJob[jobname];
        delete this._garbageCollection[jobname];
        delete this._jobCassandraIORetryStack[jobname];
        delete this._jobDoneCallbacks[jobname];
        delete this._jobTxCount[jobname];
        delete this._receivedBlocksPerJob[jobname];
        delete this._jobErrors[jobname];
    }

    _clearEnrichingJob(jobname) {
        delete this._jobErrors[jobname];
        delete this._totalBlocksPerJob[jobname];
        delete this._writtenBlocksPerJob[jobname];
        delete this._jobCassandraIORetryStack[jobname];
        delete this._jobDoneCallbacks[jobname];
    }

    parseBlock(keyspace, jobname, blockbuffer) {
        // header: 
        // hash,size,stripped_size,weight,number,version,merkle_root,timestamp,nonce,bits,coinbase_param,transaction_count
        
        // parse lines
        let lines = String(blockbuffer).split("\n");

        // create request object to bulk writes
        let queries = [];

        // abort if job is broken
        if(this._jobErrors.hasOwnProperty(jobname)==false || this._jobErrors[jobname].length>0) {
            return;
        }

        // for each line
        for(let i=0;i<lines.length;i++) {
            // buffer to parse blocks
            let blockObj = {};
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
                        
                        // increment block count to know when all block were received
                        this._receivedBlocksPerJob[jobname]++;
                        if(this._receivedBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                            this._jobTxCount[jobname].blocks_finished = true;
                        }

                        // the following statement is if by any change on any chain there is a block with a few tx
                        // and that these txs arrived before the block data

                        // if we have finished the block, send it for a write
                        if(this._blockTransactionMaps[jobname][blockObj.number].writen_tx>=this._blockTransactionMaps[jobname][blockObj.number].total_tx) {
                            this._writeBlockTransactionSummary(keyspace, jobname, blockObj.number);
                        }
                    } else { this._debug("A block was received with already set total_tx !"); }
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
        this._expressCassandraDrivers[keyspace].doBatch(queries, (err)=>{
            if(err)this._manageCassandraErrorsForJob(jobname, err, queries, "block");
        });
    }

    // function to format data for the block_transactions table
    _addTransactionToBlockSummary(keyspace, jobname, height, hash, nin, nout, tin, tout) {
        // abort if job is aborted
        if(this._jobErrors.hasOwnProperty(jobname)==false) {
            return;
        }
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
            // create the map to store transactions infos in their blocks
            // voluntarely omit total_tx for block parser function to detect tx was received before the block
            this._blockTransactionMaps[jobname][height] = {
                writen_tx: 1,
                tx_summary_list: [{
                    tx_hash: hash,
                    no_inputs: nin,
                    no_outputs: nout,
                    total_input: tin,
                    total_output: tout
                }]
            };
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
                this._manageCassandraErrorsForJob(jobname, err, [height], "block_transactions");
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
                        this._recoverFillJobErrors(keyspace, jobname);
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
        // push total number of tx and blocks in redis for later incr after enrich
        // TODO: error handling with marking job as broken if job data already got cleared and on redis errors?
        this._redisClient.set(""+this._currency.toUpperCase()+"::job_stats::"
          + jobname.split("::")[0]+"::"+jobname.split("::")[3], this._jobTxCount[jobname].txToWrite);
        // call the callback for when everything's done
        this._jobDoneCallbacks[jobname]();
        // stream updates to cli clients
        this._redisClient.publish("BTC::done", jobname);
    }

    _manageCassandraErrorsForJob(jobname, err, rows=null, table=null) {
        this._logErrors("Cassandra error at write:"+err);
        // if the write was a pending one that hadn't been finished before another failure that cleared job memory
        if(typeof this._jobErrors[jobname] == "undefined") {
            this._debug("Job "+jobname+" received a cassandra error after its termination.");
            return;
        }
        //if the job is still running save the error if we didn't saved too much already
        if(this._jobCassandraIORetryStack[jobname].count<MAX_RETRY_CASSANDRA) {
            if(rows!=null) {
                for(let i=0;i<rows.length;i++) {
                    this._jobCassandraIORetryStack[jobname][table].push(rows[i]);
                    this._jobCassandraIORetryStack[jobname].count++;
                }
            }
        // if the job has reached maximum allowed write errors, push an error to the stack
        } else {
            this._jobErrors[jobname].push("FATAL ERROR: cassandra error rate is too high, error recovery stack max size has been reached.");
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

        // abort if job is broken
        if(this._jobErrors.hasOwnProperty(jobname)==false || this._jobErrors[jobname].length>0) {
            return;
        }

        // callbacks to send the writes once UTXO have been set/get in cache

        let callbackWrite = ()=>{
            // abort if job is broken
            if(this._jobErrors.hasOwnProperty(jobname)==false || this._jobErrors[jobname].length>0) {
                return;
            }
            // send the writes
            this._cassandraDrivers[keyspace].batch(queries, {prepare:true}).then(()=>{
                // failure or not, we need to know when all tx have been received to start recovering
                // hence the count and test
                this._jobTxCount[jobname].txReceived+=queries.length;
                if(this._jobTxCount[jobname].txReceived>=this._jobTxCount[jobname].txToWrite) {
                    // if all blocks have been received and filled with txs
                    if(this._jobTxCount[jobname].blocks_finished==true) {
                        // if all block_transaction rows were written
                        if(this._writtenBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                            // start the process of recovering eventual write errors and terminating job
                            this._recoverFillJobErrors(keyspace, jobname);
                        }
                    }
                }
            }).catch((err)=>{
                this._manageCassandraErrorsForJob(jobname, err, queries, "transaction");
                this._jobTxCount[jobname].txReceived+=queries.length;
                if(this._jobTxCount[jobname].txReceived>=this._jobTxCount[jobname].txToWrite) {
                    // if all blocks have been received and filled with txs
                    if(this._jobTxCount[jobname].blocks_finished==true) {
                        // if all block_transaction rows were written
                        if(this._writtenBlocksPerJob[jobname]>=this._totalBlocksPerJob[jobname]) {
                            // start the process of recovering eventual write errors and terminating job
                            this._recoverFillJobErrors(keyspace, jobname);
                        }
                    }
                }
            });
        };

        // keep track of the tx to write and written
        let txToWrite = 0;
        let txWriten = 0;

        // create request object to bulk writes
        let queries = [];        

        // for each line
        for(let i=0;i<lines.length;i++) {
            let row;
            let jsonObj;
            // ignore headers
            if(lines[i]!="") {
                try  {
                    jsonObj = JSON.parse(lines[i]);
                    row = {
                        tx_prefix: jsonObj.hash.substring(0,5),
                        tx_hash: Buffer.from(jsonObj.hash,"hex"),
                        tx_index: String(this._generateTxIndex(jobname, jsonObj.block_number)),
                        height: jsonObj.block_number,
                        timestamp: jsonObj.block_timestamp,
                        coinbase: jsonObj.is_coinbase,
                        total_input: String(jsonObj.input_value),
                        total_output: String(jsonObj.output_value),
                        inputs: this._inputConvertETLtoGraphSense(jsonObj.inputs),
                        outputs: this._inputConvertETLtoGraphSense(jsonObj.outputs),
                        coinjoin: false
                    };

                    txToWrite++;

                    // try to see if input required are already in redis RAM cache, and push outputs in there
                    this._getSetTxIO(row.coinbase ,row.tx_hash, jsonObj.inputs, row.outputs).then((txIOresponse)=>{

                        // abort if job is aborted
                        if(this._jobErrors.hasOwnProperty(jobname)==false  || this._jobErrors[jobname].length>0) {
                            return;
                        }

                        txWriten++;

                        // if we found corresponding utxo in redis
                        if(txIOresponse.input_filled==true) {
                            // report the inputs data
                            row.inputs = txIOresponse.inputs;
                            // detect coinjoin
                            row.coinjoin = this._detectCoinjoin(row);
                            //if(DUMP_METRICS)this._redisClient.INCR("1st-input-cache::success",1);
                        } else if(jsonObj.inputs.length!=0) {
                            let inputList = [];
                            // build input list
                            for(let j=0;j<jsonObj.inputs.length;j++) {
                                inputList.push([jsonObj.inputs[j].spent_transaction_hash,jsonObj.inputs[j].spent_output_index]);
                            }
                            // if not, mark this transaction for later enrichment
                            this._redisClient.lpush(this._currency+"::"+keyspace+"::btxs::"+jsonObj.block_number, 
                                JSON.stringify({h:row.tx_hash,t:inputList, o:row.outputs}), (errLP,resLP)=>{
                                    if(errLP) {
                                        this._logErrors("ERRROR: Unable to push block job data to redis:"+errLP);
                                    }
                                });
                            //if(DUMP_METRICS)this._redisClient.INCR("1st-input-cache::failure",1);
                        // a transaction can also have no input and be a coinbase
                        } else {
                            row.inputs = [];
                            // obviously, a purely coinbase tx is not a coinjoin
                            row.coinjoin = false;
                        }

                        // save the tx summary to our maps (also increase writtenTx)
                        this._addTransactionToBlockSummary(keyspace, jobname, row.height, row.tx_hash,
                            row.inputs.length, row.outputs.length, row.total_input,
                            row.total_output);

                        // initalize request row and prepare it to be sent with others
                        queries.push({
                            query: push_transaction_query,
                            params: [row.tx_prefix, row.tx_hash, row.tx_index, row.height, row.timestamp, row.coinbase, row.total_input, row.total_output, row.inputs, row.outputs, row.coinjoin]
                        });

                        // if the transaction was a garbage collection, clean the garbage
                        if(garbageCollection==true) {
                            this._garbageCollection[jobname] = [];
                        }

                        if(this._garbageCollection[jobname].length!=0) {
                            this._logErrors("A transaction worked despise the garbage collection list not being empty!!");
                            process.exit(1);
                        }

                        // if we are done, call the final callback
                        if(txWriten>=txToWrite)callbackWrite();
                    });
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
    }

    // save the tx_outputs in redis txhash::inputid - addresses_nb,addresses,value,type
    // should take on about 1Gb of additional RAM not counting Redis fragmentation if saving
    // 1000 blocks worth of tx output with required fields: 1000*2200*(7*(32+25+8+1)) = 1Gb
    // out of 23 tx input sampled, 21 were referencing less than 1000 blocks old output
    // so 1000 sounds like a good size/sucess compromise    
    _getSetTxIO(coinbase ,tx_hash, inputs, outputs) {
        return new Promise((resolve,reject)=>{
            // TODO: implement bulked calls to a redis cache
            resolve({input_filled:false});
        });
    }

    // this function generate id with format [HEIGHT]0..0[TX_COUNT] as string
    _generateTxIndex(jobname, height) {
        let txid = "00000";
        // in case the tx is the first, it should be set to zero 
        if(this._blockTransactionMaps[jobname].hasOwnProperty(height)==false) {
            txid = "0";
        } else {
            // get the tx count for this block as a string
            txid = String(this._blockTransactionMaps[jobname][height].writen_tx);
        }

        // we want to have a fixed string length and will pad with zeros 
        while(txid.length<5) txid = "0"+txid;
        // now we can return the id
        return ""+height+txid;
    }

    _recoverEnrichJobErrors(keyspace, jobname) {

        this._debug("Started error recovery routine...");

        let recoveredCount = 0;
        let failedCount = 0;
        let totalToRecover = this._jobCassandraIORetryStack[jobname].count;

        // if there are not errors to recover, do nufin
        if(totalToRecover==0) {
            this._debug("No error to recover, cleaning up job auxiliary objects...");
            this._jobDoneCallbacks[jobname](null);
            this._clearEnrichingJob(jobname);
            return;
        }

        // callback to register successes and failures and detect the end
        let recoveredCallback = (success)=>{
            if(success==true) {
                recoveredCount++;
            } else {
                failedCount++;
            }
            // if the two bulk calls are done
            if((recoveredCount+failedCount)>=totalToRecover) {
                // if we were unable to do the failed write again
                // push a fatal job error
                if(failedCount!=0) {
                    // fire the end of job callback
                    this._jobDoneCallbacks[jobname](new Error("FATAL ERROR: job had "+
                    totalToRecover+" failed cassandra writes and could not recover some."+
                    " Watch your cassandra error rate."));
                    this._clearEnrichingJob(jobname);
                    return;
                } else {
                    // fire the end of job callback
                    this._jobDoneCallbacks[jobname](null);
                    this._clearEnrichingJob(jobname);
                    return;
                }
            }
        };

        // try to get input which failed again
        for(let i=0;i<this._jobCassandraIORetryStack[jobname]["inputs-reads"].length;i++) {
            // used to bulk calls
            let inputData = this._jobCassandraIORetryStack[jobname]["inputs-reads"][i];
            // call the function to do the cassandra requests
            this._findTxInputs(keyspace, jobname, inputData, recoveredCount, true);
        }

        // try to recover tx writes
        for(let i=0;i<this._jobCassandraIORetryStack[jobname]["input-write"].length;i++) {
            // get the query saved at previous failure
            let row = this._jobCassandraIORetryStack[jobname]["input-write"][i];
            // update the row
            this._cassandraDrivers[keyspace].execute(update_tx_inputs_query, row, {prepare: true})
                .then(()=>{
                    recoveredCallback(true);
                }).catch((err)=>{
                    this._logErrors(err);
                    recoveredCallback(false);
                });
        }
    }

    _recoverFillJobErrors(keyspace, jobname) {

        this._debug("Started error recovery routine...");

        let recoveredCount = 0;
        let failedCount = 0;
        let totalToRecover = this._jobCassandraIORetryStack[jobname].count;

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
        for(let i=0;i<this._jobCassandraIORetryStack[jobname]["block_transactions"].length;i++) {
            // used to bulk calls
            let height = this._jobCassandraIORetryStack[jobname]["block_transactions"][i];
            // push it to the list of calls to make
            blockTransacQueries.push(new this._blockTransactionsModels[keyspace]({
                height: height,
                txs: this._blockTransactionMaps[jobname][height].tx_summary_list
            }).save({return_query: true}));
        }
        // send all the block transac
        this._expressCassandraDrivers[keyspace].doBatch(blockTransacQueries, (err)=>{
            if(err) {
                this._logErrors(err);
                recoveredCallback(false);
            } else {
                recoveredCallback(true);
            }
        });

        // send block for rewrite
        let blockQueries = [];
        for(let i=0;i<this._jobCassandraIORetryStack[jobname]["block"].length;i++) {
            // used to bulk calls
            let row = this._jobCassandraIORetryStack[jobname]["block"][i];
            // push it to the list of calls to make
            blockQueries.push(new this._blockModels[keyspace](row).save({return_query: true}));
        }
        // send all the block transac
        this._expressCassandraDrivers[keyspace].doBatch(blockQueries, (err)=>{
            if(err) {
                this._logErrors(err);
                recoveredCallback(false);
            } else {
                recoveredCallback(true);
            }
        });

        // send block for rewrite
        let txQueries = [];
        for(let i=0;i<this._jobCassandraIORetryStack[jobname]["transaction"].length;i++) {
            // used to bulk calls
            let row = this._jobCassandraIORetryStack[jobname]["transaction"][i];
            // push it to the list of calls to make
            txQueries.push(row);
        }
        // send all the block transac
        this._cassandraDrivers[keyspace].batch(txQueries, {prepare: true}).then(()=>{
            recoveredCallback(true);
        }).catch((err)=>{
            this._logErrors(err);
            recoveredCallback(false);
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
            let value = "0";
            if(etlObj[i].value!=null)value=String(etlObj[i].value);
            graphsenseObj.push({
                address: etlObj[i].addresses,
                value: value,
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

    clearFillingJobIfExists(jobname) {
        // clear all auxilary objects for this job
        if (this._blockTransactionMaps.hasOwnProperty(jobname) == true)
            delete this._blockTransactionMaps[jobname];
        if (this._writtenBlocksPerJob.hasOwnProperty(jobname) == true)
            delete this._writtenBlocksPerJob[jobname];
        if (this._totalBlocksPerJob.hasOwnProperty(jobname) == true)
            delete this._totalBlocksPerJob[jobname];
        if (this._garbageCollection.hasOwnProperty(jobname) == true)
            delete this._garbageCollection[jobname];
        if (this._jobCassandraIORetryStack.hasOwnProperty(jobname) == true)
            delete this._jobCassandraIORetryStack[jobname];
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
            this._expressCassandraDrivers[keyspace].doBatch(queries, (err)=>{
                if(err) {
                    this._logErrors(err);
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    writeKeyspaceStatistics(keyspace, block_count, tx_count) {
        this.prepareForKeyspace(keyspace).then(()=>{
            this._cassandraDrivers[keyspace].execute(write_stats_query, 
                [keyspace, Number(block_count), BigInt(tx_count), Date.now()/1000],
                {prepare: true})
                .catch((err)=>{
                    // this error should be recoved at next try
                    this._logErrors("Cassandra error while trying to write statistics");
                    this._logErrors(err);
                });
        }).catch((err)=>{
            this._logErrors("Unable to get keyspace ready to write keyspace stats");
            this._logErrors(err);
        });
    }
}



module.exports = {CassandraWriter};
