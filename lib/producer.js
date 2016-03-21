'use strict';

/**
 * @namespace Consumer
 */
var uuid = require('node-uuid'),
  utils = require('./helpers/utils'),
  channel = require('./helpers/channel'),
  connect = require('./helpers/connection'),
  parsers = require('./helpers/message-parsers');

var hostname = process.env.HOSTNAME;

if (!hostname) {
  hostname = uuid.v4();
  Logger.info('[BMQ-PRODUCER]', 'Set hostname id to ' + hostname, '. Hostname can be changed using the env var HOSTNAME.');
}

module.exports = function(config) {
    let channel = channel(config);
    let rpcQueues = {};
    let maybeAnswer = function(queue) {
        return (_msg) => {
          var corrIdA = _msg.properties.correlationId;
          if (rpcQueues[queue][corrIdA] !== undefined) {
            _msg = parsers.in(_msg);
            rpcQueues[queue][corrIdA].resolve(_msg);
            Logger.info('[BMQ-PRODUCER][' + queue + '] < ', _msg);
            delete rpcQueues[queue][corrIdA];
          }
        };
      };
    let checkRpc = function(queue, msg, options) {
        options.persistent = true;

        if (options.rpc) {
          if (!rpcQueues[queue]) {
            rpcQueues[queue] = {};
          }

          return createRpcQueue(queue)
          .then(() => {
            var corrId = uuid.v4();
            options.correlationId = corrId;
            options.replyTo = rpcQueues[queue].queue;

            channel.get().sendToQueue(queue, msg, options);

            rpcQueues[queue][corrId] = Promise.defer();
            return rpcQueues[queue][corrId].promise;
          });
        }

        return channel.get().sendToQueue(queue, msg, options);
      };
    let createRpcQueue = function(queue) {
        if (rpcQueues[queue].queue) return Promise.resolve();

        var resQueue = queue + ':' + hostname + ':res';
        return channel.get().assertQueue(resQueue, { durable: true, exclusive: true })
        .then((_queue) => {
          rpcQueues[queue].queue = _queue.queue;
          return channel.get().consume(_queue.queue, maybeAnswer(queue), { noAck: true });
        })
        .then(() => {
          return queue;
        });
      };

  return {
    produce: function(queue, msg, options) {
      return Promise.resolve([config, channel.get()])
      .then(connect)
      .then(channel.create)
      .then(() => {
        if (!msg) return;
        options = options || { persistent: true, durable: true };

        Logger.info('[BMQ-PRODUCER][' + queue + '] > ', msg);

        return checkRpc(queue, parsers.out(msg, options), options);
      })
      .catch((err) => {
        Logger.error('[BMQ-PRODUCER]', err);
        return utils.timeoutPromise(config.amqpTimeout)
          .then(() => {
            return this.produce(queue, msg, options);
          });
      });
    }
  };
};
