var connection,
  amqp = require('amqplib');

/**
 * Connect method used to connect the global connection object, when disconnected (not set). To be used inside a promise chain.
 * @return {Promise} When not connected, connects and returns a promise that resolve with the amqp connection object as parameter
 * @return {object} When connected, return the amqp connection object directly
 *
 * @example
 * return Promise.resolve([config, channel])
 *   .then(connect)
 *   .then((conn) => {
 *     //conn is an amqp connection object
 *   });
 */
module.exports = (params) => {
  //params[0] is configuration
  //params[1] is channel
  if (!connection) {
    //if not connected, we unset the channel because a connection loss is a channel loss
    params[1] = null;
    //we store a promise in the global connect object and we return it, so callers can resolve when trully connected
    connection = new Promise((resolve, reject) => {
      amqp.connect(params[0].amqpUrl, { clientProperties: { hostname: process.env.HOSTNAME } })
      .then((_connection) => {

        //on connection close unset the amqpConnection object so we can reconnect
        _connection.on('close', (err) => {
          Logger.error(err);
          connection = null;
        });

        _connection.on('error', Logger.error);

        //now stores the connection object in the global var so future calls to connect can resolve immediatly with the connect object
        connection = _connection;
        //resolve the previously returned promises for producers that called the connect method before being connected
        resolve(connection);
      })
      .catch((err) => {
        //unset the connection object and reject the connection promise so all previous calls to connect will fail
        connection = null;
        reject(err);
      });
    });
  }

  //return either the connection object or the promise that will resolve once connected
  return connection;
};
