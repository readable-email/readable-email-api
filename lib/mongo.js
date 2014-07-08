'use strict';

var assert = require('assert');
var mongod = require('mongod');
var assertions = require('./assertions.js');

module.exports = MongoDatabase;
function MongoDatabase(db) {
  this.db = mongod(db, ['lists', 'messages', 'topics', 'processed']);
}

MongoDatabase.prototype.processed = function (url) {
  assert(typeof url === 'string', 'The url must be a string.');
  assert(/^http/.test(url), 'The url must start with http');
  return this.db.processed.find({_id: url}).count().then(function (count) {
    return count !== 0;
  }, function () {
    return false;
  });
};
MongoDatabase.prototype.markProcessed = function (url) {
  assert(typeof url === 'string', 'The url must be a string.');
  assert(/^http/.test(url), 'The url must start with http');
  return this.db.processed.update({_id: url}, {_id: url}, {upsert: true});
};


MongoDatabase.prototype.addMessage = function (message) {
  assertions.isMessage(message);
  return this.db.messages.update({_id: message._id}, message, {upsert: true});
};
MongoDatabase.prototype.getMessages = function (subjectToken) {
  assert(typeof subjectToken === 'string');
  return this.db.messages.find({subjectToken: subjectToken}).sort({date: 1});
};

MongoDatabase.prototype.getTopic = function (id) {
  return this.db.topics.findOne({_id: id});
};
MongoDatabase.prototype.updateSubject = function (subject) {
  assertions.isSubject(subject);
  return this.db.topics.update({_id: subject._id}, subject, {upsert: true});
};

MongoDatabase.prototype.getLists = function () {
  return this.db.lists.find();
};
MongoDatabase.prototype.getList = function (id) {
  return this.db.lists.findOne({_id: id});
};

MongoDatabase.prototype.getPage = function (source, page, numberPerPage) {
  return this.db.topics.find().sort({end: -1}).skip(page * numberPerPage).limit(numberPerPage + 1)
  .then(function (res) {
    res.first = page === 0;
    res.last = res.length < numberPerPage + 1;
    if (!res.last) res.pop();
    return res;
  });
};
MongoDatabase.prototype.close = function () {
  this.db.close();
};
