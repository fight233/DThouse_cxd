var DThouseConnection = require('./nodetaos/connection.js')
module.exports.connect = function (connection={}) {
  return new DThouseConnection(connection);
}
