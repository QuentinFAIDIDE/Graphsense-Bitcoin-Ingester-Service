// the upper and lower limit where binomial distrib function verify newton's method requirements
let minProbBinom = 0.15;
let maxProbBinom = 0.7;

// default to 12 healthchecks per minute in case of problems
let DEFAULT_HEALTHCHECK_CHANCE = 0.25;

// max newton method iteration number to consider it impossible
// should converge geometrically
let NEWTON_MAX_ITER = 2000;
// thresold for iteration differences after which we consider we converged
let NEWTON_ERR_THRESOLD = 0.001;


function findDistributedProtocolFreq(probability, replicas) {
    /*
     * compute the optimal distributed process rate per replica to have as much as probability parameter
     * chances of having two or more replicas health checking together in one step (most probably 1 second)
     * using the newton-raphson method to find root of an equation with the binomial distribution probability mass function P(X>=2)
     *
     * This implies we spawn the process as a bernouilli event of returned proba at each interval (or equivalent)
     * 
     * @returns rate per interval to maintain with required parameters
     */

    // if probability parameter is out of range, reply with default
    if(probability<0.00001 && probability>0.9995) {
        console.error("findDistributedProtocolFreq received out of range probability");
        // if it did, fallback to default
        return DEFAULT_HEALTHCHECK_CHANCE/replicas;
    }

    // if single replica, return the probability directly
    if(replicas==1) {
        return probability;
    }

    // define iteration function
    let fixedPointIteration = function (x) {
        // x_{p+1} = x_p - f(x)/f'(x)
        let fx = 1 - probability - ((1-x)**(replicas-1))*((1-x)+replicas*x);
        let fpx = (replicas-1)*x*replicas*((1-x)**(replicas-2));
        if(Number.isNaN( fx/fpx )==true) return x;
        return (x - (fx/fpx));
    };
    // now let's iterate to converge to the root
    let niter = 0;
    let sqerr = 1;
    let xn = 1/(replicas-1);
    // while we have not wasted too much trials or found satisfying result
    while(niter<NEWTON_MAX_ITER && sqerr>NEWTON_ERR_THRESOLD) {
        // compute next x
        let new_x = fixedPointIteration(xn);
        niter++;
        sqerr = Math.sqrt((xn-new_x)**2);
        xn = new_x;
    }
    // now we are done, check if it failed (shouldn't)
    if(niter>=NEWTON_MAX_ITER || xn<0 || xn>1) {
        // if it did, fallback to default
        return DEFAULT_HEALTHCHECK_CHANCE/replicas;
    }
    // if we have our probability of service occurence per second per replica
    // return it
    return xn;
}

module.exports = { findDistributedProtocolFreq };