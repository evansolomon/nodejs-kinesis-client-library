/**
 * @fileoverview Model of a single shard lease.
 */

var vogels = require('vogels')
var _ = require('underscore')

var aws = require('../aws/factory')

/**
 * Create a DynamoDB model of the lease.
 *
 * @param  {string}        tableName
 * @param  {AWS.DynamoDB}  dynamodb
 */
function createModel(tableName, dynamodb) {
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

/**
 * Shard lease model.
 *
 * @param {string}  shardId
 * @param {number}  expectedLeaseCounter
 * @param {string}  tableName
 * @param {Object}  awsConfig
 * @param {Boolean} local
 */
function Lease(shardId, expectedLeaseCounter, tableName, awsConfig, local) {
  var dynamodb = aws.create(awsConfig, local, 'DynamoDB')

  this.Model = createModel(tableName, dynamodb)
  this.shardId = shardId
  this.expectedLeaseCounter = expectedLeaseCounter
}

/**
 * Type field in the database
 *
 * @constant
 * @type {String}
 */
Lease.DB_TYPE = 'lease'

/**
 * Fetch the last checkpointed sequence number from the network database.
 *
 * @param {Function}  callback
 */
Lease.prototype.getCheckpoint = function (callback) {
  this.Model.get(Lease.DB_TYPE, this.shardId, {
    ConsistentRead: true,
    AttributesToGet: ['checkpointedSequence']
  }, function (err, lease) {
    if (err) return callback(err)

    callback(null, lease.get('checkpointedSequence'))
  })
}

/**
 * Update the lease in the network database.
 *
 * @param {Object}    properties
 * @param {Function}  callback
 */
Lease.prototype._update = function (properties, callback) {
  var atts = _.extend({
    type: Lease.DB_TYPE,
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

/**
 * Reserve the lease for this consumer.
 *
 * @param {Function}  callback
 */
Lease.prototype.reserve = function (callback) {
  this._update({}, callback)
}

/**
 * Save a checkpoint in the network database.
 *
 * @param {string}    checkpointedSequence
 * @param {Function}  callback
 */
Lease.prototype.checkpoint = function (checkpointedSequence, callback) {
  // Skip redundant writes
  if (checkpointedSequence === this.checkpointedSequence) {
    return process.nextTick(callback)
  }

  this._update({checkpointedSequence: checkpointedSequence}, callback)
}

/**
 * Mark the lease as finished so that other consumers will not try to process it.
 * @param {Function}  callback
 */
Lease.prototype.markFinished = function (callback) {
  this._update({isFinished: true}, callback)
}

/**
 * Fetch all leases from the network database.
 *
 * @static
 * @param {string}    tableName
 * @param {Object}    awsConfig
 * @param {Boolean}   local
 * @param {Function}  callback
 */
Lease.fetchAll = function (tableName, awsConfig, local, callback) {
  var dynamodb = aws.create(awsConfig, local, 'DynamoDB')
  createModel(tableName, dynamodb).query(Lease.DB_TYPE)
    .loadAll()
    .exec(callback)
}

module.exports = Lease
