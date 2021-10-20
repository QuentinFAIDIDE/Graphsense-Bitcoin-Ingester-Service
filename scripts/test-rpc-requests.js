const { getBlockHash, streamBlockAndTransactions, streamBlockRange } = require("../libs/bitcoin-nodes.js");


process.env.CRYPTO_CLIENTS_LOGIN="masterofcoin";
process.env.CRYPTO_CLIENTS_PWD="SheiTa0eOoM8thah";
let node = "10.35.33.60:31749";

getBlockHash(node, 140000).then((hash)=>{
    streamBlockAndTransactions(node, hash, (r)=>{
        console.log(r);
    }, (r)=>{
        console.log(JSON.stringify(r,null,2));
    });
});