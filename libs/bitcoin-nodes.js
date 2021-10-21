const request = require("request");
const WaitGroup = require("waitgroup");

// environement variables with btc clients logins
// CRYPTO_CLIENTS_LOGIN
// CRYPTO_CLIENTS_PWD

// used to convert btc to stats
// we use round to prevent tiny processors arythmetic errors from not returning integers
// the float operation done on some C-based software will not exactly give same result as here 
let satsMultiplier = (1/0.00000001);
let btcToSats = (btc)=>{return Math.round(satsMultiplier*Number(btc));};

function getBlockHash(btcClient, height) {
    return new Promise((resolve,reject)=>{
        nodeRpcApiWrapper(btcClient, "getblockhash", [height]).then((blockh)=>{
            resolve(blockh);
        });
    });
}

function streamBlockAndTransactions(btcClient, blockHash, blockCallback, transactionCallback) {
    return new Promise((resolve,reject)=>{
        nodeRpcApiWrapper(btcClient, "getblock", [blockHash, 2]).then((block_and_txs)=>{
            // send the blockCallback its block
            blockCallback({
                hash: block_and_txs.hash,
                number: block_and_txs.height,
                timestamp: block_and_txs.time,
                transaction_count: block_and_txs.nTx
            });
            // iterate over transaction and send em (with an index!)
            for(let i=0;i<block_and_txs.tx.length;i++) {
                // parse and transform outputs
                let outputList = [];
                let sumOutput = 0;
                for(let j=0; j<block_and_txs.tx[i].vout.length;j++) {
                    sumOutput += block_and_txs.tx[i].vout[j].value;
                    outputList.push({
                        addresses: block_and_txs.tx[i].vout[j].scriptPubKey.addresses,
                        type: block_and_txs.tx[i].vout[j].scriptPubKey.type,
                        value: btcToSats(block_and_txs.tx[i].vout[j].value)
                    }); 
                }

                // parse and transform inputs
                let inputList = [];
                // ignore coinbase
                if(i!=0) {
                    for(let j=0; j<block_and_txs.tx[i].vin.length;j++) {
                        // push a new input to the list 
                        inputList.push({
                            spent_transaction_hash: block_and_txs.tx[i].vin[j].txid,
                            spent_output_index: block_and_txs.tx[i].vin[j].vout
                        }); 
                    }
                }
                // send all the info to the callback
                transactionCallback({
                    hash: block_and_txs.tx[i].hash,
                    block_number: block_and_txs.height,
                    block_timestamp: block_and_txs.time,
                    index: generateTxIndex(block_and_txs.height, i),
                    output_value: btcToSats(sumOutput),
                    outputs: outputList,
                    inputs: inputList,
                    is_coinbase: i==0
                });
            }
            resolve();
        });
    });
}

function streamBlockRange(btcClient, minHeight, maxHeight, blockCallback, transactionCallback) {
    return new Promise((resolve,reject)=>{
        // we will recursively call a function to iterate over blocks
        let iter = Number(minHeight);

        let getNextBlock = ()=>{
            // get block hash
            getBlockHash(btcClient, iter).then((bhash)=>{
                // stream block data en iterate
                streamBlockAndTransactions(btcClient, bhash, blockCallback, transactionCallback)
                    .then(()=>{
                        // increment count
                        iter++;
                        // if above maxHeight
                        if(iter>Number(maxHeight)) {
                            // resolve and abort
                            resolve();
                            return;
                        } else {
                            // go to next block
                            getNextBlock();
                        }
                    });
            });
        };
        getNextBlock();
    });
}

function nodeRpcApiWrapper(btcClient, command, parameters) {
    return new Promise((resolve,reject)=>{
        // start a request to the rpc api
        var dataString = JSON.stringify({
            "jsonrpc": "1.0",
            "id": "curltext",
            "method": command,
            "params": parameters
        });
        // build the REST request
        var options = {
            url: `http://${process.env.CRYPTO_CLIENTS_LOGIN}:${process.env.CRYPTO_CLIENTS_PWD}@${btcClient}/`,
            method: "POST",
            headers: {"content-type": "text/plain;"},
            body: dataString
        };

        request(options, (error, response, body)=>{

            // if errors, exit and print it
            if(error) {
                reject(error);
                return;
            }
            if(response.statusCode!=200) {
                console.error("BTC Client error with code on "+command+": "+response.statusCode);
                console.error(JSON.parse(body).error.message);
                reject(new Error("Unexpected bitcoin client status code "+response.statusCode+" with message: "+JSON.parse(body).error.message));
                return;
            }
            // we parse the json and return it while handling errors
            let obj;
            try {
                obj = JSON.parse(body);
            } catch(parseErr) {
                console.error("Invalid JSON returned by Bitcoin RPC Api");
                reject(parseErr);
                return;
            }
            resolve(obj.result);
        });
    });
}

function generateTxIndex(height, iter) {
    let txid = String(iter);
    // we want to have a fixed string length and will pad with zeros 
    while(txid.length<5) txid = "0"+txid;
    // now we can return the id
    return ""+height+txid;
}

module.exports = { getBlockHash, streamBlockAndTransactions, streamBlockRange };