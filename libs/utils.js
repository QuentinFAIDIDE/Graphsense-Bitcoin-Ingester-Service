function checkRequiredEnVar(envar, name) {
    if(typeof envar == "undefined" || envar=="") {
        console.error("No "+name+" provided");
        process.exit(1);
    }
}

module.exports = { checkRequiredEnVar };