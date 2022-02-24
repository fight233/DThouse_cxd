const DThouseCursor = require('./cursor')
const CTaosInterface = require('./cinterface')
module.exports = DThouseConnection;

/**
 * DThouse Connection Class
 * @param {object} options - Options for configuring the connection with DThouse
 * @return {DThouseConnection}
 * @class DThouseConnection
 * @constructor
 * @example
 * //Initialize a new connection
 * var conn = new DThouseConnection({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
 *
 */
function DThouseConnection(options) {
  this._conn = null;
  this._host = null;
  this._user = "root"; //The default user
  this._password = "taosdata"; //The default password
  this._database = null;
  this._port = 0;
  this._config = null;
  this._chandle = null;
  this._configConn(options)
  return this;
}
/**
 * Configure the connection to DThouse
 * @private
 * @memberof DThouseConnection
 */
DThouseConnection.prototype._configConn = function _configConn(options) {
  if (options['host']) {
    this._host = options['host'];
  }
  if (options['user']) {
    this._user = options['user'];
  }
  if (options['password']) {
    this._password = options['password'];
  }
  if (options['database']) {
    this._database = options['database'];
  }
  if (options['port']) {
    this._port = options['port'];
  }
  if (options['config']) {
    this._config = options['config'];
  }
  this._chandle = new CTaosInterface(this._config);
  this._conn = this._chandle.connect(this._host, this._user, this._password, this._database, this._port);
}
/** Close the connection to DThouse */
DThouseConnection.prototype.close = function close() {
  this._chandle.close(this._conn);
}
/**
 * Initialize a new cursor to interact with DThouse with
 * @return {DThouseCursor}
 */
DThouseConnection.prototype.cursor = function cursor() {
  //Pass the connection object to the cursor
  return new DThouseCursor(this);
}
DThouseConnection.prototype.commit = function commit() {
  return this;
}
DThouseConnection.prototype.rollback = function rollback() {
  return this;
}
/**
 * Clear the results from connector
 * @private
 */
/*
 DThouseConnection.prototype._clearResultSet = function _clearResultSet() {
  var result = this._chandle.useResult(this._conn).result;
  if (result) {
    this._chandle.freeResult(result)
  }
}
*/
