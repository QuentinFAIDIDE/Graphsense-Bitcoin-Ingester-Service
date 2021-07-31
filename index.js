#!/usr/bin/env node

"use strict";

// import utilitaries
var { checkRequiredEnVar } = require("./libs/utils.js");
var randomstring = require("randomstring");
// ENVIRONMENT VARIABLES
// redis configuration
var REDIS_HOST = process.env.REDIS_HOST;
var REDIS_PORT = process.env.REDIS_PORT;
// check for existence of redis config envars
checkRequiredEnVar(REDIS_HOST, "REDIS_HOST");
checkRequiredEnVar(REDIS_PORT, "REDIS_PORT");

var USE_REDIS_PASSWORD = !(typeof process.env.REDIS_PASSWORD == "undefined");
var REDIS_PASSWORD = process.env.REDIS_PASSWORD;

// currency config
var CURRENCY_SYMBOL = process.env.CURRENCY_SYMBOL;
checkRequiredEnVar(CURRENCY_SYMBOL, "CURRENCY_SYMBOL");
// check that we support the symbol user wants to use
if(CURRENCY_SYMBOL!="BTC") {
    console.error("CURRENCY_SYMBOL "+CURRENCY_SYMBOL+" is not supported");
    process.exit(1);
}

// now the crypto client config will be checked
var CRYPTO_CLIENTS = process.env.CRYPTO_CLIENTS;
checkRequiredEnVar(CRYPTO_CLIENTS, "CRYPTO_CLIENTS");
// we then parse the list of crypto clients (comma separated list of ip:port )


// let's now check the credentials
var CRYPTO_CLIENTS_LOGIN = process.env.CRYPTO_CLIENTS_LOGIN;
var CRYPTO_CLIENTS_PWD = process.env.CRYPTO_CLIENTS_PWD;
checkRequiredEnVar(CRYPTO_CLIENTS_PWD, "CRYPTO_CLIENTS_PWD");
checkRequiredEnVar(CRYPTO_CLIENTS_LOGIN, "CRYPTO_CLIENTS_LOGIN");

// cassandra required ENVARS
var CASSANDRA_CONTACT_POINTS = process.env.CASSANDRA_CONTACT_POINTS;
var CASSANDRA_PORT = process.env.CASSANDRA_PORT;
var CASSANDRA_DATACENTER = process.env.CASSANDRA_DATACENTER;
checkRequiredEnVar(CASSANDRA_CONTACT_POINTS, "CASSANDRA_CONTACT_POINTS");
checkRequiredEnVar(CASSANDRA_PORT, "CASSANDRA_PORT");
checkRequiredEnVar(CASSANDRA_DATACENTER, "CASSANDRA_DATACENTER");

// connect to the redis instance
const redis = require("redis");
var redisClient;
if(USE_REDIS_PASSWORD==false) {
    redisClient = redis.createClient({port: REDIS_PORT, host: REDIS_HOST});
} else {
    redisClient = redis.createClient({port: REDIS_PORT, host: REDIS_HOST, password: REDIS_PASSWORD});
}

const { ReplicaScheduler } = require("./libs/replica-scheduler.js");
const { MasterRole } = require("./libs/master-role.js");
const { WorkerRole } = require("./libs/worker-role.js");

let logId = randomstring.generate(16);

// detect if debug mode enabled
var DEBUG_MODE = process.env.DEBUG_MODE;
var STREAM_DEBUG_LOGS = process.env.STREAM_DEBUG_LOGS; 
let debugLog = function (log) {};
if(typeof DEBUG_MODE == "string" && DEBUG_MODE=="true") {
    debugLog = function (debug) {
        console.log(""+Date.now()+"(debug): "+debug);
        if(typeof STREAM_DEBUG_LOGS == "string" && STREAM_DEBUG_LOGS=="true") {
            redisClient.publish(CURRENCY_SYMBOL+"::debug", ""+Date.now()+"(debug "+logId+"): "+debug);
        }
    };
}



// message logging function
var logMessages = function (msg) { console.log(""+Date.now()+": "+msg);};

// this function detect error object to dump the stack trace in it as well
let msg = "";
var logErrors = function (err) {
    if (typeof err === "object") {
        if (err.message) {
            msg = ""+Date.now()+"(error): "+err.message;
            console.log(msg);
        }
        if (err.stack) {
            console.log("\nStacktrace:");
            console.log("====================");
            console.log(err.stack);
        }
    } else {
        msg = ""+Date.now()+"(error): "+err;
        console.log(msg);
    }
    // forward the error to cli clients
    redisClient.publish(""+CURRENCY_SYMBOL.toUpperCase()+"::errors", msg, ()=>{
        // let's abort if cassandra is unreachable
        if(msg.indexOf("All host(s) tried for query failed. First host tried")!=-1) {
            console.error("Fatal error: cassandra unreachable.");
            process.exit(1);
        }
    });
};

// create master and worker managers
var masterManager = new MasterRole(REDIS_HOST, REDIS_PORT, CURRENCY_SYMBOL, logMessages, logErrors, debugLog);
var workerManager = new WorkerRole(REDIS_HOST, REDIS_PORT, CURRENCY_SYMBOL, logMessages, logErrors, debugLog);

// create replica scheduler
let replicaScheduler = new ReplicaScheduler(REDIS_HOST, REDIS_PORT, CURRENCY_SYMBOL, logMessages, logErrors, debugLog, ()=>{masterManager.startMaster();});
// start it
replicaScheduler.startScheduler();
workerManager.startWorker();

// in case of unhandled rejection, abort
process.on("unhandledRejection", err => {
    logErrors(err);
    console.log("Terminating because of unhandled promise rejection.");
    process.exit(1);
});