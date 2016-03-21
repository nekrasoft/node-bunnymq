var connect = require('./connection');

module.exports = function(_config) {
  return {
    channel: null,
    get: () => {
      return this.channel;
    },
    /**
     * createChannel method used to create a channel once connected. To be used inside a promise chain.
     * @return {Promise} When channel not created, returns a promise that resolve with the amqp channel object as parameter once channel is created
     * @return {object} When channel is created, return the channel object directly
     * 
     * @example 
     * return Promise.resolve([config, channel])
     *   .then(connect)
     *   .then(channel.create)
     *   .then((channel) => {
     *     //channel is an amqp channel object
     *   });
     */
    create: function() {
      if (!this.channel) {
        //if there is no channel created, create a promise and temporarly returns it
        this.channel = new Promise((resolve, reject) => {
          connect().createChannel()
            .then((_channel) => {
              _channel.prefetch(_config.amqpPrefetch);

              Logger.info('[BMQ-PRODUCER] Is now connected and ready to produce messages');

              //on connect close, unset the channel so it can be created again later. Connection may not have been closed so there is no need to unset it.
              _channel.on('close', (err) => {
                Logger.error(err);
                this.channel = null;
              });

              _channel.on('error', Logger.error);

              //resolve previously returned promises with the channel and store channel inside the global channel object
              this.channel = _channel;
              resolve(this.channel);
            })
            .catch((err) => {
              //if an error occured, unset the channel and reject the previously returned promises
              this.channel = null;
              reject(err);
            });
        });
      }

      //return either a promise when channel does not exists, or the channel object directly
      return this.channel;
    }
  };
};
