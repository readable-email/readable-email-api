'use strict';

var assert = require('assert');

exports.isMessage = function (message) {
  assert(typeof message._id === 'string', 'message._id should be a string');
  assert(/^http/.test(message._id, 'message._id should be a url'));
  assert(typeof message.subjectToken === 'string', 'message.subjectToken should be a string');
  assert(typeof message.source === 'string', 'message.source must be a string');
  assert(message._id.substr(0, message.source.length) === message.source, 'message._id should start with message.source');
    assert(message.subjectToken.substr(0, message.source.length) === message.source, 'message.subjectToken should start with message.source');

  assert(typeof message.subject === 'string', 'message.subject should be a string');
  exports.isFrom(message.from, 'message.from');
  assert(message.date instanceof Date, 'message.date should be an instance of Date');
  assert(typeof message.body === 'string', 'message.body should be a string');
};

exports.isSubject = function (subject) {
  assert(typeof subject._id === 'string');
  assert(typeof subject.source === 'string', 'subject.source must be a string');
  assert(subject._id.substr(0, subject.source.length) === subject.source, 'subject._id should start with subject.source');
  assert(typeof subject.subjectToken === 'string');
  assert(typeof subject.subject === 'string');
  assert(typeof subject.messages === 'number');
  assert(subject.messages > 0);
  exports.isFrom(subject.first, 'subject.first');
  exports.isFrom(subject.last, 'subject.last');
  assert(subject.start instanceof Date);
  assert(subject.end instanceof Date);
};

exports.isFrom = function (from, prop) {
  assert(from && typeof from === 'object', prop + ' should be an object');
  assert(typeof from.email === 'string', prop + '.email should be a string');
  assert(typeof from.name === 'string', prop + '.name should be a string');
};
