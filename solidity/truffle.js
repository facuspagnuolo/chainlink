require('babel-register');
require('babel-polyfill');

global.h = require('./helpers');

module.exports = {
  network: "test",
  networks: {
    local: {
      host: 'localhost',
      port: 8545,
      network_id: '*',
      gas: 5000000,
      gasPrice: 2e9, // 2 Gwei
    },
    ropsten: {
      host: 'localhost',
      port: 8565,
      network_id: 3,
      gas: 500000,
      gasPrice: 10e9, // 10 Gwei
    },
    rinkeby: {
      host: 'localhost',
      port: 8565,
      network_id: 4,
      gas: 500000,
      gasPrice: 10e9, // 10 Gwei
    },
    kovan: {
      host: 'localhost',
      port: 8555,
      network_id: 42,
      gas: 500000,
      gasPrice: 10e9, // 10 Gwei
    },
  }
};
