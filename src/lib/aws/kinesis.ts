import async = require('async')
import AWS = require('aws-sdk')

export interface ListShardsCallback {(err: any, data?: AWS.kinesis.Shard[]): void}
const listShards = (client: AWS.Kinesis, stream: string, callback: ListShardsCallback) => {
  let shards = []
  let foundAllShards = false
  var startShardId

  function next(done) {
    const params = {
      StreamName: stream,
      ExclusiveStartShardId: startShardId
    }

    client.describeStream(params, function (err, data) {
      if (err) {
        return done(err)
      }

      if (! data.StreamDescription.HasMoreShards) {
        foundAllShards = true
      }

      const lastShard = data.StreamDescription.Shards[data.StreamDescription.Shards.length - 1]
      startShardId = lastShard.ShardId

      shards = shards.concat(data.StreamDescription.Shards)
      done()
    })
  }

  const test = () => {
    return !! foundAllShards
  }

  const finish = err => {
    if (err) {
      return callback(err)
    }
    callback(null, shards)
  }

  async.doUntil(next, test, finish)
}

export {listShards}
