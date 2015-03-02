import AWS = require('aws-sdk')
import vogels = require('vogels')
import _ = require('underscore')

import awsFactory = require('../aws/factory')

function createModel(tableName: string, dynamodb: AWS.DynamoDB) {
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

export class Model {
  public Lease: vogels.Model
  public shardId: string
  public expectedLeaseCounter: number
  private checkpointedSequence: string
  private static DB_TYPE = 'lease'

  constructor(shardId: string, counter: number, table: string, conf: AWS.ClientConfig, local: Boolean) {
    var dynamodb = awsFactory.dynamo(conf, local)

    this.Lease = createModel(table, dynamodb)
    this.shardId = shardId
    this.expectedLeaseCounter = counter
  }

  public getCheckpoint (callback: (err: any, checkpoint?: string) => void) {
    this.Lease.get(Model.DB_TYPE, this.shardId, {
      ConsistentRead: true,
      AttributesToGet: ['checkpointedSequence']
    }, function (err, lease) {
      if (err) return callback(err)

      callback(null, lease.get('checkpointedSequence'))
    })
  }

  private _update (properties: Object, callback: (err: any) => void) {
    var atts = _.extend({
      type: Model.DB_TYPE,
      id: this.shardId,
      leaseCounter: {$add: 1},
      expiresAt: Date.now() + (1000 * 15)
    }, properties)

    var expected = {
      expected: {leaseCounter: this.expectedLeaseCounter}
    }

    this.expectedLeaseCounter = (this.expectedLeaseCounter || 0) + 1

    this.Lease.update(atts, expected, function (err, record) {
      if (! err) {
        this.checkpointedSequence = record.get('checkpointedSequence')
      }

      callback.apply(null, arguments)
    }.bind(this))
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

  public static fetchAll (tableName: string, conf: AWS.ClientConfig, local: Boolean,
                          callback: (err: any, data: vogels.Queries.Query.Result) => void
  ) {
    var dynamodb = awsFactory.dynamo(conf, local)
    createModel(tableName, dynamodb).query(Model.DB_TYPE)
      .loadAll()
      .exec(callback)
  }
}
