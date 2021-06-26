module.exports = {
    "env": {
        "node": true,
        "commonjs": true,
        "es6": true
    },
    "parserOptions": {
        "ecmaVersion": 8 
    },
    "extends": "eslint:recommended",
    "rules": {
        "indent": [
            "error",
            4
        ],
        "linebreak-style": [
            "error",
            "unix"
        ],
        "quotes": [
            "error",
            "double"
        ],
        "semi": [
            "error",
            "always"
        ],
        "no-console": "off",
        "no-unused-vars": "off"
    }
};