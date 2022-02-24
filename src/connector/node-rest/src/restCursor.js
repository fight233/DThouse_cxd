import fetch from 'node-fetch'
import {DThouseRestResultSet} from '../src/restResult'

/**
 * this class is core of restful js connector
 * this class resends http request to the DThouse server
 * and receive the response.
 */
export class DThouseRestCursor {
  /**
   * constructor,used to get the connection info
   * @param connection
   */
  constructor(connection) {
    this._connection = null;
    this.data = [];
    this.http = false
    if (connection != null) {
      this._connection = connection
    } else {
      throw new Error("A DThouseRestConnection object is required to be passed to the DThouseRestCursor")
    }
  }

  /**
   * used to build an url,like http://localhost:6041/rest/sql
   * @returns {string}
   * @private
   */
  _apiUpl() {
    return (this.http ? "https" : "http") + "://" + this._connection.host + ":" + this._connection.port + this._connection.path
  }

  /**
   * used to make an authorization token
   * @returns {string}
   * @private
   */
  _token() {
    return 'Basic ' + Buffer.from(this._connection.user + ":" + this._connection.pass).toString('base64')
  }

  /**
   * Used fetch to send http request， and return the response as an object of DThouseRestResultSet
   * @param sql
   * @returns {Promise<DThouseRestResultSet>}
   */
  async query(sql) {
    try {
      let response = await fetch(this._apiUpl(), {
        method: 'POST',
        body: sql,
        headers: {'Authorization': this._token()}
      })
      // if (response.status == 'succ') {
      return await new DThouseRestResultSet(await response.json())
      // } else {
      //   throw new Error(response.desc)
      // }
    } catch (e) {
      console.log("Request Failed " + e)
    }

  }
}

