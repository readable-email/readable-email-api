'use strict';

var MongoDatabase = require('./lib/mongo.js');

module.exports = connect;
function connect(str) {
  return new MongoDatabase(str);
}
