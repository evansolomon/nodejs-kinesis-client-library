var os = require('os')

var async = require('async')
var vogels = require('vogels')
var _ = require('underscore')

var awsFactory = require('../aws/factory')

var getModel = function (tableName, dynamodb) {
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

function Cluster(tableName, awsConfig) {
  this.id = [os.hostname(), process.pid, Date.now()].join('@')
  var dynamodb = awsFactory(awsConfig, 'DynamoDB')
  this.Model = getModel(tableName, dynamodb)
}

Cluster.prototype.reportActiveConsumers = function (activeConsumers, callback) {
  this.Model.update({
    type: 'cluster',
    id: this.id,
    expiresAt: Date.now() + (1000 * 15),
    activeConsumers: activeConsumers
  }, callback)
}

Cluster.prototype.fetchAll = function (callback) {
  this.Model.query('cluster')
    .filter('expiresAt').gt(Date.now())
    .loadAll()
    .exec(callback)
}

Cluster.prototype.garbageCollect = function (callback) {
  var _this = this
  this.Model.query('cluster')
    .filter('expiresAt').lt(Date.now())
    .loadAll()
    .exec(function (err, clusters) {
      if (err) return callback(err)

      async.each(clusters.Items, function (cluster, done) {
        _this.Model.destroy('cluster', cluster.get('id'), done)
      }, callback)
    })
}


Cluster.createTable = function (tableName, awsConfig, callback) {
  var dynamodb = awsFactory(awsConfig, 'DynamoDB')

  var model = getModel(tableName, dynamodb)
  var tableStatus

  model.createTable({
    readCapacity: 10,
    writeCapacity: 10
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

Cluster.tableExists = function (tableName, awsConfig, callback) {
  var dynamodb = awsFactory(awsConfig, 'DynamoDB')

  getModel(tableName, dynamodb).describeTable(function (err) {
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
