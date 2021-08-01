const assert = require("assert");
const redis = require("redis");
const { MasterRole } = require("../libs/master-role.js");

// import utilitaries
var { checkRequiredEnVar } = require("../libs/utils.js");

var REDIS_HOST = process.env.REDIS_HOST;
var REDIS_PORT = process.env.REDIS_PORT;
// check for existence of redis config envars
checkRequiredEnVar(REDIS_HOST, "REDIS_HOST");
checkRequiredEnVar(REDIS_PORT, "REDIS_PORT");

var redisClient = redis.createClient({port: REDIS_PORT, host: REDIS_HOST});

describe("Master job and messaging functions", function () {

    this.timeout(5000);

    it("should detect one CALLROLL protocol response", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST", (log)=>{}, (log)=>{}, (log)=>{} );
        master._parseChannelMessage("TEST::000000::idwork::CALLROLL::response,idwork,"+master._identifier);
        assert.strictEqual(master._workersIds.length, 1);
        assert.strictEqual(master._workersDelay.length, 1);
        assert.strictEqual(master._workersIds[0], "idwork");
        done();
    });

    it("should post a message in the right format", (done)=>{
        let subClient = redis.createClient({port: REDIS_PORT, host: REDIS_HOST});
        subClient.subscribe("TEST1::job-scheduling");

        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST1", (log)=>{}, (log)=>{}, (log)=>{} );

        subClient.on("message", (channel, msg)=>{
            let msgSplit=msg.split("::");
            assert.strictEqual(channel=="TEST1::job-scheduling" && msgSplit[0]=="TEST1"
              && msgSplit[2]==master._identifier && msgSplit[3]=="TESTACTION" && msgSplit[4]=="testparam",true);
            done();
        });

        master._sendMessage("TESTACTION","testparam");
    });

    it("should post 150 jobs", (done)=>{

        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST2", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.sadd("TEST2::monitored-keyspaces", "ks1");
        mult1.del("TEST2::monitored::ks1");
        mult1.hmset("TEST2::monitored::ks1", "feedFrom", 0, "lastQueuedBlock", -1, "delay", 10, "name", "ks1", "lastFilledBlock", -1);

        master._checkActiveJobLiveliness = ()=>{
            return new Promise((resolve,reject)=>{resolve();});
        };
        master._checkErrorStack = ()=>{
            return new Promise((resolve,reject)=>{resolve();});
        };
        master._getLastAvailableBlock = ()=>{
            return new Promise((resolve,reject)=>{resolve(600000);});
        };
        master._checkTimedOutJobs = ()=>{
            return new Promise((resolve,reject)=>{resolve();});
        };
        let yesterday = master._getYesterdayDateTxt();
        let lastRateIngested = "";
        master._updateRatesForKeyspaceForRange = (keyspace, from, to) =>{
            lastRateIngested = to;
        };

        mult1.exec((errEx,resEx)=>{
            master._runJobCheck();

            setTimeout(()=>{
                redisClient.lrange("TEST2::jobs::todo", 0, -1, (errLR1,resLR1)=>{
                    redisClient.lrange("TEST2::jobs::posted", 0, -1, (errLR2,resLR2)=>{
                        assert.strictEqual(lastRateIngested, yesterday);
                        assert.strictEqual(resLR1.length, 150);
                        assert.strictEqual(resLR2.length, 150);
                        done();
                    });
                });
            }, 1000);
        });

    });
});

describe("Master job timeout recovery functionalities", function () {

    it("should recover a job and put it back in the todo stack", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST3", console.log, console.log, console.log);
        let mult1 = redisClient.multi();

        mult1.del("TEST3::jobs::doing");
        mult1.del("TEST3::jobs::todo");
        mult1.del("TEST3::jobs::posted");
        mult1.lpush("TEST3::jobs::doing", "ks1::JOB_NAME::alive-worker::0,1999");
        mult1.lpush("TEST3::jobs::doing", "ks1::JOB_NAME::dead-worker::2000,3999");
        mult1.lpush("TEST3::jobs::posted", "0000::ks1::JOB_NAME::0,1999");
        mult1.lpush("TEST3::jobs::posted", "0000::ks1::JOB_NAME::2000,3999");

        master._getActiveWorkers = ()=>{
            return new Promise((resolve,reject)=>{resolve(["alive-worker"])});
        };

        mult1.exec((errEx,resEx)=>{
            master._checkActiveJobLiveliness().then(()=>{
                master._checkActiveJobLiveliness().then(()=>{
                    // get redis tables
                    let mult2 = redisClient.multi();
                    mult2.lrange("TEST3::jobs::doing", 0, -1);
                    mult2.lrange("TEST3::jobs::todo", 0, -1);
                    mult2.lrange("TEST3::jobs::posted", 0, -1);
                    mult2.exec((errM2,resM2)=>{
                        // doing still has the job from alive worker
                        assert.strictEqual(resM2[0].length,1);
                        assert.strictEqual(resM2[0][0],"ks1::JOB_NAME::alive-worker::0,1999");
                        // todo has a new job
                        assert.strictEqual(resM2[1].length,1);
                        assert.strictEqual(resM2[1][0],"ks1::JOB_NAME::2000,3999");
                        // posted has updated posting date for reset job
                        assert.strictEqual(resM2[2].length,2);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::0,1999"), true);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::2000,3999"), false);
                        done();
                    });
                });
            });
        });
    });


    it("should recover all jobs", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST4", console.log, console.log, console.log);
        let mult1 = redisClient.multi();

        mult1.del("TEST4::jobs::doing");
        mult1.del("TEST4::jobs::todo");
        mult1.del("TEST4::jobs::posted");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker1::0,1999");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker2::2000,3999");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker3::4000,5999");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker4::6000,7999");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker5::8000,9999");
        mult1.lpush("TEST4::jobs::doing", "ks1::JOB_NAME::dead-worker6::10000,11999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::0,1999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::2000,3999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::4000,5999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::6000,7999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::8000,9999");
        mult1.lpush("TEST4::jobs::posted", "0000::ks1::JOB_NAME::10000,11999");

        master._getActiveWorkers = ()=>{
            return new Promise((resolve,reject)=>{resolve([])});
        };

        mult1.exec((errEx,resEx)=>{
            master._checkActiveJobLiveliness().then(()=>{
                master._checkActiveJobLiveliness().then(()=>{
                    // get redis tables
                    let mult2 = redisClient.multi();
                    mult2.lrange("TEST4::jobs::doing", 0, -1);
                    mult2.lrange("TEST4::jobs::todo", 0, -1);
                    mult2.lrange("TEST4::jobs::posted", 0, -1);
                    mult2.exec((errM2,resM2)=>{
                        // doing is now empty
                        assert.strictEqual(resM2[0].length,0);
                        // todo has new jobs
                        assert.strictEqual(resM2[1].length,6);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::0,1999"), true);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::2000,3999"), true);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::4000,5999"), true);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::6000,7999"), true);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::8000,9999"), true);
                        assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::10000,11999"), true);
                        // posted has updated posting date for reset job
                        assert.strictEqual(resM2[2].length,6);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::0,1999"), false);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::2000,3999"), false);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::4000,5999"), false);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::6000,7999"), false);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::8000,9999"), false);
                        assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::1000,11999"), false);
                        done();
                    });
                });
            });
        });
    });

    it("should recover all jobs that timed out", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST5", console.log, console.log, console.log);
        let mult1 = redisClient.multi();

        mult1.del("TEST5::jobs::doing");
        mult1.del("TEST5::jobs::todo");
        mult1.del("TEST5::jobs::posted");
        mult1.lpush("TEST5::jobs::doing", "ks1::JOB_NAME::dead-worker1::0,1999");
        mult1.lpush("TEST5::jobs::doing", "ks1::JOB_NAME::dead-worker2::2000,3999");
        mult1.lpush("TEST5::jobs::doing", "ks1::JOB_NAME::dead-worker3::4000,5999");
        mult1.lpush("TEST5::jobs::doing", "ks1::JOB_NAME::dead-worker4::6000,7999");
        mult1.lpush("TEST5::jobs::todo", "ks1::JOB_NAME::8000,9999");
        mult1.lpush("TEST5::jobs::todo", "ks1::JOB_NAME::10000,11999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::0,1999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::2000,3999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::4000,5999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::6000,7999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::8000,9999");
        mult1.lpush("TEST5::jobs::posted", "0000::ks1::JOB_NAME::10000,11999");

        mult1.exec((errEx,resEx)=>{
            master._checkTimedOutJobs().then(()=>{
                // get redis tables
                let mult2 = redisClient.multi();
                mult2.lrange("TEST5::jobs::doing", 0, -1);
                mult2.lrange("TEST5::jobs::todo", 0, -1);
                mult2.lrange("TEST5::jobs::posted", 0, -1);
                mult2.exec((errM2,resM2)=>{
                    // doing is now empty
                    assert.strictEqual(resM2[0].length,0);
                    // todo has all jobs
                    assert.strictEqual(resM2[1].length,6);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::0,1999"), true);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::2000,3999"), true);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::4000,5999"), true);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::6000,7999"), true);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::8000,9999"), true);
                    assert.strictEqual(resM2[1].includes("ks1::JOB_NAME::10000,11999"), true);
                    // posted has updated posting date for reset jobs
                    assert.strictEqual(resM2[2].length,6);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::0,1999"), false);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::2000,3999"), false);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::4000,5999"), false);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::6000,7999"), false);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::8000,9999"), false);
                    assert.strictEqual(resM2[2].includes("0000::ks1::JOB_NAME::1000,11999"), false);
                    done();
                });
            });
        });
    });
});

describe("Test the filled ranges management", function () {

    it("should post 3 enrichment job and set lastFilledBlock to 7999", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST6", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.del("TEST6::filled-ranges::ks1");
        mult1.del("TEST6::jobs::doing");
        mult1.del("TEST6::jobs::todo");
        mult1.del("TEST6::jobs::posted");

        mult1.lpush("TEST6::filled-ranges::ks1", "10000,11999");
        mult1.lpush("TEST6::filled-ranges::ks1", "4000,5999");
        mult1.lpush("TEST6::filled-ranges::ks1", "12000,13999");
        mult1.lpush("TEST6::filled-ranges::ks1", "2000,3999");
        mult1.lpush("TEST6::filled-ranges::ks1", "4000,5999");
        mult1.lpush("TEST6::filled-ranges::ks1", "6000,7999");
        mult1.lpush("TEST6::filled-ranges::ks1", "16000,17999");
        let keyspaceobj = {
            name: "ks1",
            lastFilledBlock: 1999
        };

        let jobsToBePosted = ["ks1::ENRICH_BLOCK_RANGE::2000,3999", "ks1::ENRICH_BLOCK_RANGE::4000,5999", "ks1::ENRICH_BLOCK_RANGE::6000,7999"];
        let filledRangesRemaining = ["10000,11999", "12000,13999", "16000,17999"];

        mult1.exec((errM1,errM2)=>{
            master._updateLastFilledBlockAndPopEnrichJobs(keyspaceobj).then(()=>{
                let mult2 = redisClient.multi();
                mult2.hget("TEST6::monitored::ks1", "lastFilledBlock");
                mult2.lrange("TEST6::jobs::todo", 0, -1);
                mult2.lrange("TEST6::jobs::posted", 0, -1);
                mult2.lrange("TEST6::filled-ranges::ks1", 0, -1);
                mult2.exec((errMuL2, resMul2)=>{
                    assert.strictEqual(Number(resMul2[0]), 7999);
                    assert.strictEqual(resMul2[1].length, 3);
                    assert.strictEqual(resMul2[2].length, 3);
                    assert.strictEqual(resMul2[3].length, 3);
                    for(let i=0;i<3;i++) {
                        assert.strictEqual(jobsToBePosted.includes(resMul2[1][i]), true);
                    }
                    for(let i=0;i<3;i++) {
                        assert.strictEqual(filledRangesRemaining.includes(resMul2[3][i]), true);
                    }
                    done();
                });
            });
        });
    });


    it("should post 3 enrichment job and set lastFilledBlock to 7999", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST7", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.del("TEST7::filled-ranges::ks1");
        mult1.del("TEST7::jobs::doing");
        mult1.del("TEST7::jobs::todo");
        mult1.del("TEST7::jobs::posted");

        mult1.lpush("TEST7::filled-ranges::ks1", "0,1999");
        mult1.lpush("TEST7::filled-ranges::ks1", "0,1999");
        mult1.lpush("TEST7::filled-ranges::ks1", "0,1999");
        mult1.lpush("TEST7::filled-ranges::ks1", "10000,11999");
        mult1.lpush("TEST7::filled-ranges::ks1", "4000,5999");
        mult1.lpush("TEST7::filled-ranges::ks1", "12000,13999");
        mult1.lpush("TEST7::filled-ranges::ks1", "2000,3999");
        mult1.lpush("TEST7::filled-ranges::ks1", "4000,5999");
        mult1.lpush("TEST7::filled-ranges::ks1", "6000,7999");
        mult1.lpush("TEST7::filled-ranges::ks1", "16000,17999");
        let keyspaceobj = {
            name: "ks1",
            lastFilledBlock: 1999
        };

        let jobsToBePosted = ["ks1::ENRICH_BLOCK_RANGE::2000,3999", "ks1::ENRICH_BLOCK_RANGE::4000,5999", "ks1::ENRICH_BLOCK_RANGE::6000,7999"];
        let filledRangesRemaining = ["10000,11999", "12000,13999", "16000,17999"];

        mult1.exec((errM1,errM2)=>{
            master._updateLastFilledBlockAndPopEnrichJobs(keyspaceobj).then(()=>{
                let mult2 = redisClient.multi();
                mult2.hget("TEST7::monitored::ks1", "lastFilledBlock");
                mult2.lrange("TEST7::jobs::todo", 0, -1);
                mult2.lrange("TEST7::jobs::posted", 0, -1);
                mult2.lrange("TEST7::filled-ranges::ks1", 0, -1);
                mult2.exec((errMuL2, resMul2)=>{
                    assert.strictEqual(Number(resMul2[0]), 7999);
                    assert.strictEqual(resMul2[1].length, 3);
                    assert.strictEqual(resMul2[2].length, 3);
                    assert.strictEqual(resMul2[3].length, 3);
                    for(let i=0;i<3;i++) {
                        assert.strictEqual(jobsToBePosted.includes(resMul2[1][i]), true);
                    }
                    for(let i=0;i<3;i++) {
                        assert.strictEqual(filledRangesRemaining.includes(resMul2[3][i]), true);
                    }
                    done();
                });
            });
        });
    });

    it("should do nothing apart clearing duplicated ranges", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST8", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.del("TEST8::filled-ranges::ks1");
        mult1.del("TEST8::jobs::doing");
        mult1.del("TEST8::jobs::todo");
        mult1.del("TEST8::jobs::posted");

        mult1.lpush("TEST8::filled-ranges::ks1", "0,1999");
        mult1.lpush("TEST8::filled-ranges::ks1", "0,1999");
        mult1.lpush("TEST8::filled-ranges::ks1", "0,1999");
        mult1.hset("TEST8::monitored::ks1", "lastFilledBlock", 1999);
        let keyspaceobj = {
            name: "ks1",
            lastFilledBlock: 1999
        };

        mult1.exec((errM1,errM2)=>{
            master._updateLastFilledBlockAndPopEnrichJobs(keyspaceobj).then(()=>{
                let mult2 = redisClient.multi();
                mult2.hget("TEST8::monitored::ks1", "lastFilledBlock");
                mult2.llen("TEST8::jobs::todo");
                mult2.llen("TEST8::jobs::posted");
                mult2.llen("TEST8::filled-ranges::ks1");
                mult2.exec((errMuL2, resMul2)=>{
                    assert.strictEqual(Number(resMul2[0]), 1999);
                    assert.strictEqual(resMul2[1], 0);
                    assert.strictEqual(resMul2[2], 0);
                    assert.strictEqual(resMul2[3], 0);
                    done();
                });
            });
        });
    });
});

describe("test enriched ranges and cassandra stat writing", function () {

    it("should clear useless ranges", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST9", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.del("TEST9::ks1::enriched-ranges");

        mult1.lpush("TEST9::ks1::enriched-ranges", "0,1999");
        mult1.lpush("TEST9::ks1::enriched-ranges", "0,1999");
        mult1.lpush("TEST9::ks1::enriched-ranges", "0,1999");
        mult1.hset("TEST9::monitored::ks1", "lastEnrichedBlock", "1999");
        let keyspaceobj = {
            name: "ks1",
            lastEnrichedBlock: 1999
        };

        master._cassandraWriter.writeKeyspaceStatistics = ()=>{};

        mult1.exec((errM1,errM2)=>{
            master._updateLastEnrichedBlockAndStatistics(keyspaceobj).then(()=>{
                let mult2 = redisClient.multi();
                mult2.hget("TEST9::monitored::ks1", "lastEnrichedBlock");
                mult2.llen("TEST9::ks1::enriched-ranges");
                mult2.exec((errMuL2, resMul2)=>{
                    assert.strictEqual(Number(resMul2[0]), 1999);
                    assert.strictEqual(resMul2[1], 0);
                    done();
                });
            });
        });
    });

    it("should increase block and tx counters and delete duplicated ranges", (done)=>{
        let master = new MasterRole(REDIS_HOST, REDIS_PORT, "TEST10", console.log, console.log, console.log);
        let mult1 = redisClient.multi();
        mult1.del("TEST10::ks1::enriched-ranges");
        mult1.del("TEST10::monitored::ks1");
        mult1.del("TEST10::ks1::block-count");
        mult1.del("TEST10::ks1::tx-count");

        mult1.lpush("TEST10::ks1::enriched-ranges", "0,1999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "0,1999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "0,1999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "2000,3999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "4000,5999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "8000,9999");
        mult1.lpush("TEST10::ks1::enriched-ranges", "10000,11999");

        mult1.set("TEST10::job_stats::ks1::0,1999", 10000);
        mult1.set("TEST10::job_stats::ks1::2000,3999", 70000);
        mult1.set("TEST10::job_stats::ks1::4000,5999", 50000);
        mult1.set("TEST10::job_stats::ks1::8000,9999", 30000);


        mult1.hset("TEST10::monitored::ks1", "lastEnrichedBlock", "1999");
        let keyspaceobj = {
            name: "ks1",
            lastEnrichedBlock: 1999
        };

        let enrichedRangesRemaining = ["10000,11999","8000,9999"];

        master._cassandraWriter.writeKeyspaceStatistics = ()=>{};

        mult1.exec((errM1,resM1)=>{
            master._updateLastEnrichedBlockAndStatistics(keyspaceobj).then(()=>{
                let mult2 = redisClient.multi();
                mult2.hget("TEST10::monitored::ks1", "lastEnrichedBlock");
                mult2.lrange("TEST10::ks1::enriched-ranges", 0, -1);
                mult2.get("TEST10::ks1::block-count");
                mult2.get("TEST10::ks1::tx-count");
                mult2.exec((errMuL2, resMul2)=>{
                    assert.strictEqual(Number(resMul2[0]), 5999);
                    assert.strictEqual(resMul2[1].length, 2);
                    for(let i=0;i<2;i++) {
                        assert.strictEqual(enrichedRangesRemaining.includes(resMul2[1][i]), true);
                    }
                    assert.strictEqual(Number(resMul2[2]), 4000);
                    assert.strictEqual(Number(resMul2[3]), 70000+50000);
                    done();
                });
            }).catch(console.log);
        });
    });
});