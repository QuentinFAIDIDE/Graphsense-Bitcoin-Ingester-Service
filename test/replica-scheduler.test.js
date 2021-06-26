const assert = require("assert");
const { RedisClient } = require("redis");
const { ReplicaScheduler } = require("../libs/replica-scheduler.js");

// import utilitaries
var { checkRequiredEnVar } = require("../libs/utils.js");

var REDIS_HOST = process.env.REDIS_HOST;
var REDIS_PORT = process.env.REDIS_PORT;
// check for existence of redis config envars
checkRequiredEnVar(REDIS_HOST, "REDIS_HOST");
checkRequiredEnVar(REDIS_PORT, "REDIS_PORT");

testsrunning = 0;
function startTest() {
    testsrunning++;
}

function stopTest() {
    testsrunning--;
    if(testsrunning==0) {
        // does cleaning
    }
}


describe("Testing replica scheduler", function () {

    this.timeout(60000);

    it("5 replicas should always have one master over time", (done)=>{

        let rs = [];
        let n = 5;

        for(let i=0; i<n;i++) {
            rs.push(new ReplicaScheduler(REDIS_HOST,REDIS_PORT,"TEST", (msg)=>{console.log("Replica "+i+" log: "+msg)}, (msg)=>{console.log("Replica "+i+" err: "+msg)}, (msg)=>{console.log("Replica "+i+" debug: "+msg)},()=>{console.log("Replica "+i+" is now master")}));
        }

        for(let i=0; i<n;i++) {
            setTimeout(()=>{rs[i].startScheduler();}, Math.random()*300);
        }

        // test duration in milliseconds
        let test_duration = 35000;

        let test_start_time = Date.now();

        // probability of random death
        let pdeath = 0.3;

        let waitAndCheck = function () {
            setTimeout(()=>{
                // if test is over, stop
                if(Math.abs(Date.now()-test_start_time)>test_duration) {
                    done();
                    for(let i=0; i<n;i++) {
                        rs[i].stopScheduler();
                    }
                    return;
                }
                // test if there is one master
                let nbmaster = 0;
                for(let i=0; i<n;i++) {
                    if(rs[i].isRunningAsMaster()==true)nbmaster++;
                }
                if(nbmaster==0) {
                    assert.fail("No master replica");
                } else if (nbmaster>1) {
                    assert.fail("Multiple master replicas");
                } else {
                    // for each replica
                    for(let i=0; i<n;i++) {
                        let die = Math.random();
                        if(die<pdeath) {
                            rs[i].stopScheduler();
                            rs[i] = null;
                            console.log("Killed replica "+i);
                            rs[i]=new ReplicaScheduler(REDIS_HOST,REDIS_PORT,"TEST", (msg)=>{console.log("Replica "+i+" log: "+msg)}, (msg)=>{console.log("Replica "+i+" err: "+msg)}, (msg)=>{console.log("Replica "+i+" debug: "+msg)},()=>{console.log("Replica "+i+" is now master")});
                            setTimeout(()=>{rs[i].startScheduler();}, Math.random()*300);
                        }
                    }
                    // now, recall function
                    waitAndCheck();
                }
            }, 5000);
        }

        setTimeout(()=>{waitAndCheck();}, 10000);
    });
});
