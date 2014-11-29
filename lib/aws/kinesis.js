var async = require('async')

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

exports.getRecords = function (client, shardIterator, callback) {
  client.getRecords({
    ShardIterator: shardIterator
  }, callback)
}

exports.getShardIterator = function (client, streamName, shardId, iteratorType, sequenceNumber, callback) {
  client.getShardIterator({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: iteratorType,
    StartingSequenceNumber: sequenceNumber
  }, callback)
}
