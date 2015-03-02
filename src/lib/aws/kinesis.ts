import async = require('async')
import AWS = require('aws-sdk')

export interface listShardsCallback {(err: any, data?: AWS.kinesis.Shard[]): void}
export var listShards = function (client: AWS.Kinesis, stream: string, callback: listShardsCallback) {
  var shards = []
  var foundAllShards = false
  var startShardId

  function next(done) {
    var params = {
      StreamName: stream,
      ExclusiveStartShardId: startShardId
    }

    client.describeStream(params, function (err, data) {
      if (err) return done(err)

      if (! data.StreamDescription.HasMoreShards) {
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
