import * as os from 'os'

import * as async from 'async'
import * as vogels from 'vogels'

import * as AWS from 'aws-sdk'
import * as awsFactory from '../aws/factory'

const createModel = (tableName: string, dynamodb: AWS.DynamoDB) => {
  const Cluster = vogels.define('Cluster', schema => {
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

export class Model {
  private static DefaultCapacity = {READ: 10, WRITE: 10}
  private static DB_TYPE = 'cluster'

  public id: string
  public Cluster: vogels.Model

  public constructor (tableName: string, conf: AWS.ClientConfig, dynamoEndpoint?: string) {
    this.id = [os.hostname(), process.pid, Date.now()].join('@')

    const dynamodb = awsFactory.dynamo(conf, dynamoEndpoint)
    this.Cluster = createModel(tableName, dynamodb)
  }

  public reportActiveConsumers (activeConsumers: number, callback: (e: any) => void) {
    this.Cluster.update({
      type: Model.DB_TYPE,
      id: this.id,
      expiresAt: Date.now() + (1000 * 15),
      activeConsumers: activeConsumers
    }, callback)
  }

  public fetchAll (callback: (err: any, data: vogels.Queries.Query.Result) => void) {
    this.Cluster.query(Model.DB_TYPE)
      .filter('expiresAt').gt(Date.now())
      .loadAll()
      .exec(callback)
  }

  public garbageCollect (callback: (err: any, data?: vogels.Queries.Item[]) => void) {
    this.Cluster.query(Model.DB_TYPE)
      .filter('expiresAt').lt(Date.now())
      .loadAll()
      .exec((err, clusters) => {
        if (err) {
          return callback(err)
        }

        async.each(clusters.Items, (cluster, done) => {
          this.Cluster.destroy('cluster', cluster.get('id'), done)
        }, err => {
          if (err) {
            return callback(err)
          }

          callback(null, clusters.Items)
        })
      })
  }

  public static createTable (name: string, conf: AWS.ClientConfig, capacity: Capacity,
    dynamoEndpoint: string, callback: (e: any) => void)
  {
    const dynamodb = awsFactory.dynamo(conf, dynamoEndpoint)

    const model = createModel(name, dynamodb)
    let tableStatus

    model.createTable({
      readCapacity: capacity.read || Model.DefaultCapacity.READ,
      writeCapacity: capacity.write || Model.DefaultCapacity.WRITE
    }, err => {
      if (err) {
        return callback(err)
      }

      async.doUntil(done => {
        model.describeTable(function (err, data) {
          if (err) {
            return done(err)
          }

          tableStatus = data.Table.TableStatus
          done()
        })
      }, () => {
        return tableStatus === 'ACTIVE'
      }, callback)
    })
  }

  public static tableExists (name: string, conf: AWS.ClientConfig, dynamoEndpoint: string,
    callback: (err: any, data?: Boolean) => void)
  {
    const dynamodb = awsFactory.dynamo(conf, dynamoEndpoint)

    createModel(name, dynamodb).describeTable(err => {
      if (err && err.code === 'ResourceNotFoundException') {
        callback(null, false)
      } else if (err) {
        callback(err)
      } else {
        callback(null, true)
      }
    })
  }
}

export interface Capacity {
  read?: number;
  write?: number;
}
