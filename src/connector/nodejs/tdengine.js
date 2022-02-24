var DThouseConnection = require('./nodetaos/connection.js')
const DThouseConstant = require('./nodetaos/constants.js')
module.exports = {
  connect: function (connection = {}) {
    return new DThouseConnection(connection);
  },
  SCHEMALESS_PROTOCOL: DThouseConstant.SCHEMALESS_PROTOCOL,
  SCHEMALESS_PRECISION: DThouseConstant.SCHEMALESS_PRECISION,
}