import {hostname} from 'os'

import {each, doUntil} from 'async'
import {define, Model, Queries} from 'vogels'

import {ClientConfig, DynamoDB} from 'aws-sdk'
import {createDynamoClient} from '../aws/factory'

const createModel = (tableName: string, dynamodb: DynamoDB) => {
  const Cluster = define('Cluster', schema => {
    schema.String('type', { hashKey: true })
    schema.String('id', { rangeKey: true })
    schema.Number('activeConsumers').required()
    schema.Number('expiresAt').required()
  })

  Cluster.config({
    tableName: tableName,
    dynamodb: dynamodb,
  })

  return Cluster
}

export class Cluster {
  private static DefaultCapacity = { READ: 10, WRITE: 10 }
  private static DB_TYPE = 'cluster'

  public id: string
  public Cluster: Model

  public constructor(tableName: string, conf: ClientConfig, dynamoEndpoint?: string) {
    this.id = [hostname(), process.pid, Date.now()].join('@')

    const dynamodb = createDynamoClient(conf, dynamoEndpoint)
    this.Cluster = createModel(tableName, dynamodb)
  }

  public reportActiveConsumers(activeConsumers: number, callback: (e: any) => void) {
    this.Cluster.update({
      type: Cluster.DB_TYPE,
      id: this.id,
      expiresAt: Date.now() + (1000 * 15),
      activeConsumers: activeConsumers,
    }, callback)
  }

  public fetchAll(callback: (err: any, data: Queries.Query.Result) => void) {
    this.Cluster.query(Cluster.DB_TYPE)
      .filter('expiresAt').gt(Date.now())
      .loadAll()
      .exec(callback)
  }

  public garbageCollect(callback: (err: any, data?: Queries.Item[]) => void) {
    this.Cluster.query(Cluster.DB_TYPE)
      .filter('expiresAt').lt(Date.now())
      .loadAll()
      .exec((err, clusters) => {
        if (err) {
          return callback(err)
        }

        each(clusters.Items, (cluster, done) => {
          this.Cluster.destroy('cluster', cluster.get('id'), done)
        }, err => {
          if (err) {
            return callback(err)
          }

          callback(null, clusters.Items)
        })
      })
  }

  public static createTable(name: string, conf: ClientConfig, capacity: Capacity,
    dynamoEndpoint: string, callback: (e: any) => void
  ) {
    const dynamodb = createDynamoClient(conf, dynamoEndpoint)
    const model = createModel(name, dynamodb)
    let tableStatus

    model.createTable({
      readCapacity: capacity.read || Cluster.DefaultCapacity.READ,
      writeCapacity: capacity.write || Cluster.DefaultCapacity.WRITE,
    }, err => {
      if (err) {
        return callback(err)
      }

      doUntil(done => {
        model.describeTable((err, data) => {
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

  public static tableExists(name: string, conf: ClientConfig, dynamoEndpoint: string,
    callback: (err: any, data?: Boolean) => void) {
    const dynamodb = createDynamoClient(conf, dynamoEndpoint)

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
