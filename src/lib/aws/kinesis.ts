import {doUntil} from 'async'
import {Kinesis, kinesis} from 'aws-sdk'

export interface ListShardsCallback {
  (err: any, data?: kinesis.Shard[]): void
}

export const listShards = (client: Kinesis, stream: string, callback: ListShardsCallback) => {
  let shards = []
  let foundAllShards = false
  var startShardId

  const next = done => {
    const params = {
      StreamName: stream,
      ExclusiveStartShardId: startShardId,
    }

    client.describeStream(params, (err, data) => {
      if (err) {
        return done(err)
      }

      if (!data.StreamDescription.HasMoreShards) {
        foundAllShards = true
      }

      const lastShard = data.StreamDescription.Shards[data.StreamDescription.Shards.length - 1]
      startShardId = lastShard.ShardId

      shards = shards.concat(data.StreamDescription.Shards)
      done()
    })
  }

  const test = () => !!foundAllShards

  const finish = err => {
    if (err) {
      return callback(err)
    }

    callback(null, shards)
  }

  doUntil(next, test, finish)
}
