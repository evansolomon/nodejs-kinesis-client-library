import * as AWS from 'aws-sdk'
import * as vogels from 'vogels'
import * as _ from 'underscore'

import * as awsFactory from '../aws/factory'

const createModel = (tableName: string, dynamodb: AWS.DynamoDB) => {
  const Lease = vogels.define('Lease', schema => {
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

export class Model {
  public Lease: vogels.Model
  public shardId: string
  public expectedLeaseCounter: number
  private checkpointedSequence: string
  private static DB_TYPE = 'lease'

  constructor(shardId: string, counter: number, table: string, conf: AWS.ClientConfig, dynamoEndpoint: string) {
    const dynamodb = awsFactory.dynamo(conf, dynamoEndpoint)

    this.Lease = createModel(table, dynamodb)
    this.shardId = shardId
    this.expectedLeaseCounter = counter
  }

  public getCheckpoint (callback: (err: any, checkpoint?: string) => void) {
    this.Lease.get(Model.DB_TYPE, this.shardId, {
      ConsistentRead: true,
      AttributesToGet: ['checkpointedSequence']
    }, (err, lease) => {
      if (err) {
        return callback(err)
      }

      callback(null, lease.get('checkpointedSequence'))
    })
  }

  private _update (properties: Object, callback: (err: any) => void) {
    const atts = _.extend({
      type: Model.DB_TYPE,
      id: this.shardId,
      leaseCounter: {$add: 1},
      expiresAt: Date.now() + (1000 * 15)
    }, properties)

    const expected = {
      expected: {leaseCounter: this.expectedLeaseCounter}
    }

    this.expectedLeaseCounter = (this.expectedLeaseCounter || 0) + 1

    this.Lease.update(atts, expected, (err, record) => {
      if (! err) {
        this.checkpointedSequence = record.get('checkpointedSequence')
      }

      callback(err)
    })
  }

  public reserve (callback: (err: any) => void) {
    this._update({}, callback)
  }

  public checkpoint (checkpointedSequence: string, callback: (err: any) => void) {
    // Skip redundant writes
    if (checkpointedSequence === this.checkpointedSequence) {
      return process.nextTick(callback)
    }

    this._update({checkpointedSequence: checkpointedSequence}, callback)
  }

  public markFinished (callback: (err: any) => void) {
    this._update({isFinished: true}, callback)
  }

  public static fetchAll (tableName: string, conf: AWS.ClientConfig, dynamoEndpoint: string,
                          callback: (err: any, data: vogels.Queries.Query.Result) => void
  ) {
    const dynamodb = awsFactory.dynamo(conf, dynamoEndpoint)
    createModel(tableName, dynamodb).query(Model.DB_TYPE)
      .loadAll()
      .exec(callback)
  }
}
