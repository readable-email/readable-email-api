'use strict';

var assert = require('assert');
var crypto = require('crypto');
var mongod = require('mongod');
var Promise = require('promise');
var knox = require('knox');
var concat = require('concat-stream');
var assertions = require('./assertions.js');

module.exports = MongoDatabase;
function MongoDatabase(db, bucket) {
  this.db = mongod(db, ['lists', 'messages', 'topics', 'processed']);
  var client = knox.createClient({
    key: bucket.split('/')[0],
    secret: bucket.split('/')[1],
    bucket: bucket.split('/')[2]
  });
  this.files = {
    putBuffer: function (path, body) {
      assert(typeof path === 'string' && path.length, 'Path must be a non-empty string');
      assert(Buffer.isBuffer(body), 'Body must be a buffer');
      return new Promise(function (resolve, reject) {
        client.putBuffer(body, path, {}, function (err, res) {
          if (err) reject(err);
          else resolve(res);
        });
      });
    },
    getBuffer: function (path) {
      assert(typeof path === 'string' && path.length, 'Path must be a non-empty string');
      return new Promise(function (resolve, reject) {
        client.getFile(path, function (err, res) {
          if (err) return reject(err);
          res.on('error', reject);
          res.pipe(concat(resolve));
        });
      });
    }
  };
}

/**
 * Test if a message has been processed
 *
 * @param {String} url          The url of the original message
 * @returns {Promise.<Boolean>} `true` if the message has been processed, otherwise `false`
 */
MongoDatabase.prototype.processed = function (url) {
  assert(typeof url === 'string', 'The url must be a string.');
  assert(/^http/.test(url), 'The url must start with http');
  return this.db.processed.find({_id: url}).count().then(function (count) {
    return count !== 0;
  }, function () {
    return false;
  });
};

/**
 * Mark a message as processed
 *
 * @param {String} url The url of the original message
 * @returns {Promise}
 */
MongoDatabase.prototype.markProcessed = function (url) {
  assert(typeof url === 'string', 'The url must be a string.');
  assert(/^http/.test(url), 'The url must start with http');
  return this.db.processed.update({_id: url}, {_id: url}, {upsert: true});
};

MongoDatabase.prototype.addMessage = function (message) {
  assertions.isMessage(message);
  var body = new Buffer(message.body);
  delete message.body;

  return Promise.all([
    this.db.messages.update({_id: message._id}, message, {upsert: true}),
    this.files.putBuffer(bodyLocation(message._id) + '/original.md', body)
  ]);
};
MongoDatabase.prototype.getMessageHeaders = function (subjectToken) {
  assert(typeof subjectToken === 'string');
  return this.db.messages.find({subjectToken: subjectToken}).sort({date: 1});
};
MongoDatabase.prototype.getMessages = function (subjectToken) {
  assert(typeof subjectToken === 'string');
  var files = this.files;
  return this.db.messages.find({subjectToken: subjectToken}).sort({date: 1}).then(function (messages) {
    return Promise.all(messages.map(function (message) {
      return files.getBuffer(bodyLocation(message._id) + '/original.md').then(function (body) {
        message.body = body.toString('utf8');
        return message;
      });
    }));
  });
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
  return this.db.topics.find({source: source}).sort({end: -1}).skip(page * numberPerPage).limit(numberPerPage + 1)
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

function bodyLocation(id) {
  return crypto.createHash('sha512').update(id).digest("hex");
}
