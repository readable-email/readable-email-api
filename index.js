'use strict';

var MongoDatabase = require('./lib/mongo.js');

module.exports = connect;
function connect(db, bucket) {
  return new MongoDatabase(db, bucket);
}
