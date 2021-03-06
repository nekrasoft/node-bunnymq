var assert = require('assert');
var producer = require('../index')().producer;
var consumer = require('../index')().consumer;
var uuid = require('node-uuid');

var fixtures = {
  queues: ['rpc-queue-0', 'rpc-queue-1', 'rpc-queue-2', 'rpc-queue-3']
};

describe('Producer/Consumer RPC messaging:', function() {
  it('should be able to create a consumer that returns a message if called as RPC [rpc-queue-0]', function () {
    return consumer.consume(fixtures.queues[0], function () {
      return 'Power Ranger Red';
    })
    .then(function(created) {
      assert(created === true);
    });
  });

  it('should be able to produce a RPC message and get a response [rpc-queue-0]', function () {
    return producer.produce(fixtures.queues[0], { msg: uuid.v4() }, { rpc: true })
    .then(function (response) {
      assert.equal(response, 'Power Ranger Red');
    });
  });

  it('should be able to produce a RPC message and get a as JSON [rpc-queue-1]', function () {
    return consumer.consume(fixtures.queues[1], function () {
      return { powerRangerColor: 'Pink' };
    })
    .then(function() {
      return producer.produce(fixtures.queues[1], { msg: uuid.v4() }, { rpc: true });
    })
    .then(function (response) {
      assert.equal(typeof response, 'object');
      assert.equal(response.powerRangerColor, 'Pink');
    });
  });

  it('should be able to produce a RPC message and get undefined response [rpc-queue-2]', function () {
    return consumer.consume(fixtures.queues[2], function () {
      return undefined;
    })
    .then(function() {
      return producer.produce(fixtures.queues[2], undefined, { rpc: true });
    })
    .then(function (response) {
      assert(response === undefined, 'Got a response !== undefined');
    });
  });

  it('should be able to produce a RPC message and get null response [rpc-queue-3]', function () {
    return consumer.consume(fixtures.queues[3], function () {
      return null;
    })
    .then(function() {
      return producer.produce(fixtures.queues[3], undefined, { rpc: true });
    })
    .then(function (response) {
      assert(response === null, 'Got a response !== null');
    });
  });
});
