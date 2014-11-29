var async = require('async')
var vogels = require('vogels')
var _ = require('underscore')

var awsFactory = require('../aws/factory')

var getModel = function (tableName, dynamodb) {
  var Lease = vogels.define('Lease', function (schema) {
    schema.String('type', {hashKey: true})
    schema.String('id', {rangeKey: true})
    schema.Number('leaseCounter').required()
    schema.Number('expiresAt').required()
    schema.String('checkpointedSequence')
    schema.Boolean('isFinished')
  })

  Lease.config({
    tableName: tableName,
    dynamodb: dynamodb
  })

  return Lease
}

function Lease(shardId, expectedLeaseCounter, tableName, awsConfig) {
  var dynamodb = awsFactory(awsConfig, 'DynamoDB')
  this.Model = getModel(tableName, dynamodb)
  this.shardId = shardId
  this.expectedLeaseCounter = expectedLeaseCounter
}

Lease.prototype.getCheckpoint = function (callback) {
  this.Model.get('lease', this.shardId, {
    ConsistentRead: true,
    AttributesToGet: ['checkpointedSequence']
  }, function (err, lease) {
    if (err) return callback(err)

    callback(null, lease.get('checkpointedSequence'))
  })
}

Lease.prototype._update = function (properties, callback) {
  var atts = _.extend({
    type: 'lease',
    id: this.shardId,
    leaseCounter: {$add: 1},
    expiresAt: Date.now() + (1000 * 15)
  }, properties)

  var expected = {
    expected: {leaseCounter: this.expectedLeaseCounter}
  }

  this.expectedLeaseCounter = (this.expectedLeaseCounter || 0) + 1

  this.Model.update(atts, expected, function (err, record) {
    if (! err) {
      this.checkpointedSequence = record.get('checkpointedSequence')
    }

    callback.apply(null, arguments)
  }.bind(this))
}

Lease.prototype.reserve = function (callback) {
  this._update({}, callback)
}

Lease.prototype.checkpoint = function (checkpointedSequence, callback) {
  // Skip redundant writes
  if (checkpointedSequence === this.checkpointedSequence) {
    return process.nextTick(callback)
  }

  this._update({checkpointedSequence: checkpointedSequence}, callback)
}

Lease.prototype.markFinished = function (callback) {
  this._update({isFinished: true}, callback)
}

Lease.fetchAll = function (tableName, awsConfig, callback) {
  var dynamodb = awsFactory(awsConfig, 'DynamoDB')
  getModel(tableName, dynamodb).query('lease')
    .loadAll()
    .exec(callback)
}

module.exports = Lease
