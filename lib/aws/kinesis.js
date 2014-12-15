/**
 * @fileoverview Wrap the Kinesis API's we need.
 */

var async = require('async')

/**
 * List the shards from a Kinesis stream.
 *
 * @param {AWS.Kinesis}  client
 * @param {string}       streamName  Name of the Kinesis stream.
 * @param {Function}     callback    Array of strings on success.
 */
exports.listShards = function (client, streamName, callback) {
  var shards = []
  var foundAllShards = false
  var startShardId

  function next(done) {
    var params = {
      StreamName: streamName,
      ExclusiveStartShardId: startShardId
    }

    client.describeStream(params, function (err, data) {
      if (err) return done(err)

      if (! data.HasMoreShards) {
        foundAllShards = true
      }

      shards = shards.concat(data.StreamDescription.Shards)
      done()
    })
  }

  function test() {
    return !! foundAllShards
  }

  function finish(err) {
    if (err) return callback(err)
    callback(null, shards)
  }

  async.doUntil(next, test, finish)
}

/**
 * Get a batch of records from the stream.
 * http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
 *
 * @param {AWS.Kinesis}  client
 * @param {string}       shardIterator
 * @param {Function}     callback       Object with Records and NextShardIterator on success.
 */
exports.getRecords = function (client, shardIterator, callback) {
  client.getRecords({
    ShardIterator: shardIterator
  }, callback)
}

/**
 * Get a shard iterator for a given sequence number and iterator type.
 * http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
 *
 * @param {AWS.Kinesis} client
 * @param {string}      streamName
 * @param {string}      shardId
 * @param {string}      iteratorType
 * @param {string}      sequenceNumber
 * @param {Function}    callback        Object with ShardIterator on success.
 */
exports.getShardIterator = function (client, streamName, shardId, iteratorType, sequenceNumber, callback) {
  client.getShardIterator({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: iteratorType,
    StartingSequenceNumber: sequenceNumber
  }, callback)
}
