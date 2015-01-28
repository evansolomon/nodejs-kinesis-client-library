/**
 * @fileoverview Model of the cluster.
 */

var os = require('os')

var async = require('async')
var vogels = require('vogels')

var aws = require('../aws/factory')

/**
 * Create a DynamoDB model of the cluster.
 *
 * @param {string}        tableName
 * @param {AWS.DynamoDB}  dynamodb
 */
function createModel(tableName, dynamodb) {
  var Cluster = vogels.define('Cluster', function (schema) {
    schema.String('type', {hashKey: true})
    schema.String('id', {rangeKey: true})
    schema.Number('activeConsumers').required()
    schema.Number('expiresAt').required()
  })

  Cluster.config({
    tableName: tableName,
    dynamodb: dynamodb
  })

  return Cluster
}

/**
 * Cluster model.
 *
 * @constructor
 * @param {string}  tableName  DynamoDB table name.
 * @param {Object}  awsConfig  Configuration for AWS service instance.
 * @param {Boolean} local      Whether or not to use a local endpoint.
 */
function Cluster(tableName, awsConfig, local) {
  this.id = [os.hostname(), process.pid, Date.now()].join('@')

  var dynamodb = aws.create(awsConfig, local, 'DynamoDB')
  this.Model = createModel(tableName, dynamodb)
}

/**
 * @enum {number}
 */
Cluster.DefaultCapacity = {
  READ: 10,
  WRITE: 10
}

/**
 * Type field in the database
 *
 * @constant
 * @type {String}
 */
Cluster.DB_TYPE = 'cluster'

/**
 * Update DynamoDB with this cluster's number of consumers.
 *
 * @param {number}    activeConsumers  Number of currently-running consumers in this cluster.
 * @param {Function}  callback
 */
Cluster.prototype.reportActiveConsumers = function (activeConsumers, callback) {
  this.Model.update({
    type: Cluster.DB_TYPE,
    id: this.id,
    expiresAt: Date.now() + (1000 * 15),
    activeConsumers: activeConsumers
  }, callback)
}

/**
 * Fetch data about each cluster in the network.
 *
 * @param {Function}  callback  Called with a DynamoDB result on success.
 */
Cluster.prototype.fetchAll = function (callback) {
  this.Model.query(Cluster.DB_TYPE)
    .filter('expiresAt').gt(Date.now())
    .loadAll()
    .exec(callback)
}

/**
 * Delete clusters from the network that have expired.
 *
 * @param  {Function} callback
 */
Cluster.prototype.garbageCollect = function (callback) {
  var _this = this
  this.Model.query(Cluster.DB_TYPE)
    .filter('expiresAt').lt(Date.now())
    .loadAll()
    .exec(function (err, clusters) {
      if (err) return callback(err)

      async.each(clusters.Items, function (cluster, done) {
        _this.Model.destroy('cluster', cluster.get('id'), done)
      }, function (err) {
        if (err) return callback(err)
        callback(null, clusters.Items)
      })
    })
}

/**
 * Create the network table in DynamoDB.
 *
 * @static
 * @param {string}    tableName
 * @param {Object}    awsConfig
 * @param {Object}    capacity
 * @param {Boolean}   local
 * @param {Function}  callback
 */
Cluster.createTable = function (tableName, awsConfig, capacity, local, callback) {
  var dynamodb = aws.create(awsConfig, local, 'DynamoDB')

  var model = createModel(tableName, dynamodb)
  var tableStatus

  model.createTable({
    readCapacity: capacity.read || Cluster.DefaultCapacity.READ,
    writeCapacity: capacity.write || Cluster.DefaultCapacity.WRITE
  }, function (err) {
    if (err) return callback(err)

    async.doUntil(function (done) {
      model.describeTable(function (err, data) {
        if (err) return done(err)
        tableStatus = data.Table.TableStatus
        done()
      })
    }, function () {
      return tableStatus === 'ACTIVE'
    }, callback)
  })
}

/**
 * Determine whether the network table already exists.
 *
 * @static
 * @param {string}    tableName
 * @param {Object}    awsConfig
 * @param {Boolean}   local
 * @param {Function}  callback
 */
Cluster.tableExists = function (tableName, awsConfig, local, callback) {
  var dynamodb = aws.create(awsConfig, local, 'DynamoDB')

  createModel(tableName, dynamodb).describeTable(function (err) {
    if (err && err.code === 'ResourceNotFoundException') {
      callback(null, false)
    } else if (err) {
      callback(err)
    } else {
      callback(null, true)
    }
  })
}

module.exports = Cluster
