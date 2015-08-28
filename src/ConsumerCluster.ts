import * as events from 'events'
import * as nodeCluster from 'cluster'
import * as path from 'path'
import * as url from 'url'

import * as _ from 'underscore'
import * as async from 'async'
import * as AWS from 'aws-sdk'
import * as bunyan from 'bunyan'
import * as vogels from 'vogels'

import * as awsFactory from './lib/aws/factory'
import config from './lib/config'
import * as kinesis from './lib/aws/kinesis'
import * as lease from './lib/models/Lease'
import * as cluster from './lib/models/Cluster'
import * as stream from './lib/models/Stream'
import * as server from './lib/server'


interface ClusterWorkerWithOpts extends nodeCluster.Worker {
  opts: {shardId: string}
}

interface AWSEndpoints {
  kinesis: string
  dynamo: string
}

export interface ConsumerClusterOpts {
  streamName: string
  tableName: string
  awsConfig: AWS.ClientConfig
  dynamoEndpoint?: string
  localDynamo: Boolean
  kinesisEndpoint?: string
  localKinesis: Boolean
  localKinesisPort?: string
  capacity: cluster.Capacity
  startingIteratorType?: string
  logLevel?: string
  numRecords?: number
  timeBetweenReads?: number
}


// Cluster of consumers.
export class ConsumerCluster extends events.EventEmitter {
  public cluster: cluster.Model
  private opts: ConsumerClusterOpts
  private logger: bunyan.Logger
  private kinesis: AWS.Kinesis
  private isShuttingDownFromError = false
  private externalNetwork = {}
  private consumers = {}
  private consumerIds = []
  private lastGarbageCollectedAt = Date.now()
  private endpoints: AWSEndpoints

  constructor(pathToConsumer: string, opts: ConsumerClusterOpts) {
    super()
    this.opts = opts

    this.logger = bunyan.createLogger({
      name: 'KinesisCluster',
      level: opts.logLevel
    })

    nodeCluster.setupMaster({
      exec: pathToConsumer,
      silent: true
    })

    this.endpoints = {
      kinesis: this.getKinesisEndpoint(),
      dynamo: this.getDynamoEndpoint()
    }

    this.kinesis = awsFactory.kinesis(this.opts.awsConfig, this.endpoints.kinesis)
    this.cluster = new cluster.Model(this.opts.tableName, this.opts.awsConfig, this.endpoints.dynamo)
    this._init()
  }

  private _init () {
    async.auto({
      tableExists: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        cluster.Model.tableExists(tableName, awsConfig, this.getDynamoEndpoint(), done)
      },

      createTable: ['tableExists', (done, data) => {
        if (data.tableExists) {
          return done()
        }

        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        const capacity = this.opts.capacity || {}

        this.logger.info({table: tableName}, 'Creating DynamoDB table')
        cluster.Model.createTable(tableName, awsConfig, capacity, this.getDynamoEndpoint(), done)
      }],

      createStream: done => {
        const streamName = this.opts.streamName
        const streamModel = new stream.Stream(streamName, this.kinesis)

        streamModel.exists((err, exists) => {
          if (err) {
            return done(err)
          }

          if (exists) {
            return done()
          }

          this.kinesis.createStream({StreamName: streamName, ShardCount: 1}, err => {
            if (err) {
              return done(err)
            }

            streamModel.onActive(done)
          })
        })
      }
    }, err => {
      if (err) {
        return this._logAndEmitError(err, 'Error ensuring Dynamo table exists')
      }

      this._bindListeners()
      this._loopReportClusterToNetwork()
      this._loopFetchExternalNetwork()
    })
  }

  private getKinesisEndpoint () {
    const isLocal = this.opts.localKinesis
    const port = this.opts.localKinesisPort
    const customEndpoint = this.opts.kinesisEndpoint
    let endpoint = null

    if (isLocal) {
      const endpointConfig = config.localKinesisEndpoint
      if (port) {
        endpointConfig.port = port
      }
      endpoint = url.format(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  private getDynamoEndpoint () {
    const isLocal = this.opts.localDynamo
    const customEndpoint = this.opts.dynamoEndpoint
    let endpoint = null

    if (isLocal) {
      var endpointConfig = config.localDynamoDBEndpoint
      endpoint = url.format(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  // Run an HTTP server. Useful as a health check.
  public serveHttp (port: string|number) {
    this.logger.debug('Starting HTTP server on port %s', port)
    server.create(port, () => this.consumerIds.length)
  }

  private _bindListeners () {
    this.on('updateNetwork', () => {
      this._garbageCollectClusters()

      if (this._shouldTryToAcquireMoreShards()) {
        this.logger.debug('Should try to acquire more shards')
        this._fetchAvailableShard()
      } else if (this._hasTooManyShards()) {
        this.logger.debug({consumerIds: this.consumerIds}, 'Have too many shards')
        this._killConsumer(err => {
          if (err) {
            this._logAndEmitError(err)
          }
        })
      }
    })

    this.on('availableShard', (shardId, leaseCounter) => {
      // Stops accepting consumers, since the cluster will be reset based one an error
      if (this.isShuttingDownFromError) {
        return
      }

      this._spawn(shardId, leaseCounter)
    })

  }

  // Compare cluster state to external network to figure out if we should try to change our shard allocation.
  private _shouldTryToAcquireMoreShards () {
    if (this.consumerIds.length === 0) {
      return true
    }

    const externalNetwork = this.externalNetwork
    const networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return true
    }

    const lowestInOutterNetwork = networkKeys.reduce((memo, key) => {
      const count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length <= lowestInOutterNetwork
  }

  // Determine if we have too many shards compared to the rest of the network.
  private _hasTooManyShards () {
    const externalNetwork = this.externalNetwork

    const networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return false
    }

    const lowestInOutterNetwork = networkKeys.reduce((memo, key) => {
      const count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length > (lowestInOutterNetwork + 1)
  }

  // Fetch data about unleased shards.
  private _fetchAvailableShard () {
    // Hack around typescript
    var _asyncResults = <{allShardIds: string[]; leases: vogels.Queries.Query.Result;}> {}

    async.parallel({
      allShardIds: done => {
        kinesis.listShards(this.kinesis, this.opts.streamName, (err, shards) => {
          if (err) {
            return done(err)
          }

          const shardIds = _.pluck(shards, 'ShardId')
          _asyncResults.allShardIds = shardIds
          done()
        })
      },
      leases: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        lease.Model.fetchAll(tableName, awsConfig, this.getDynamoEndpoint(), (err, leases) => {
          if (err) {
            return done(err)
          }

          _asyncResults.leases = leases
          done()
        })
      }
    }, err => {
      if (err) {
        return this._logAndEmitError(err, 'Error fetching available shards')
      }

      const allShardIds = _asyncResults.allShardIds
      const leases = _asyncResults.leases
      const leaseItems = leases.Items

      const finishedShardIds = leaseItems.filter(lease => {
        return lease.get('isFinished')
      }).map(lease => {
        return <string> lease.get('id')
      })

      const allUnfinishedShardIds = allShardIds.filter(id => {
        return finishedShardIds.indexOf(id) === -1
      })

      const leasedShardIds = leaseItems.map(item => {
        return item.get('id')
      })
      const newShardIds = _.difference(allUnfinishedShardIds, leasedShardIds)

      // If there are shards theat have not been leased, pick one
      if (newShardIds.length > 0) {
        this.logger.info({newShardIds: newShardIds}, 'Unleased shards available')
        return this.emit('availableShard', newShardIds[0], null)
      }

      // Try to find the first expired lease
      let currentLease
      for (var i = 0; i < leaseItems.length; i++) {
        currentLease = leaseItems[i]
        if (currentLease.get('expiresAt') > Date.now()) {
          continue
        }
        if (currentLease.get('isFinished')) {
          continue
        }

        let shardId = currentLease.get('id')
        let leaseCounter = currentLease.get('leaseCounter')
        this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Found available shard')
        return this.emit('availableShard', shardId, leaseCounter)
      }
    })
  }

  // Create a new consumer processes.
  private _spawn (shardId: string, leaseCounter: number) {
    this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Spawning consumer')
    const consumerOpts = {
      tableName: this.opts.tableName,
      awsConfig: this.opts.awsConfig,
      streamName: this.opts.streamName,
      startingIteratorType: (this.opts.startingIteratorType || '').toUpperCase(),
      shardId: shardId,
      leaseCounter: leaseCounter,
      dynamoEndpoint: this.endpoints.dynamo,
      kinesisEndpoint: this.endpoints.kinesis,
      numRecords: this.opts.numRecords,
      timeBetweenReads: this.opts.timeBetweenReads,
      logLevel: this.opts.logLevel
    }

    const env = {
      CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
      CONSUMER_SUPER_CLASS_PATH: path.join(__dirname, 'AbstractConsumer.js')
    }

    const consumer = <ClusterWorkerWithOpts> nodeCluster.fork(env)
    consumer.opts = consumerOpts
    consumer.process.stdout.pipe(process.stdout)
    consumer.process.stderr.pipe(process.stderr)
    this._addConsumer(consumer)
  }

  // Add a consumer to the cluster.
  private _addConsumer (consumer: ClusterWorkerWithOpts) {
    this.consumerIds.push(consumer.id)
    this.consumers[consumer.id] = consumer

    consumer.once('exit', code => {
      const logMethod = code === 0 ? 'info' : 'error'
      this.logger[logMethod]({shardId: consumer.opts.shardId, exitCode: code}, 'Consumer exited')

      this.consumerIds = _.without(this.consumerIds, consumer.id)
      delete this.consumers[consumer.id]
    })
  }

  // Kill any consumer in the cluster.
  private _killConsumer (callback: (err: any) => void) {
    const id = this.consumerIds[0]
    this._killConsumerById(id, callback)
  }

  // Kill a specific consumer in the cluster.
  private _killConsumerById (id: number, callback: (err: any) => void) {
    this.logger.info({id: id}, 'Killing consumer')

    let callbackWasCalled = false
    const wrappedCallback = (err: any) => {
      if (callbackWasCalled) {
        return
      }
      callbackWasCalled = true
      callback(err)
    }

    // Force kill the consumer in 40 seconds, giving enough time for the consumer's shutdown
    // process to finish
    const timer = setTimeout(() => {
      if (this.consumers[id]) {
        this.consumers[id].kill()
      }
      wrappedCallback(new Error('Consumer did not exit in time'))
    }, 40000)

    this.consumers[id].once('exit', code => {
      clearTimeout(timer)
      let err = null
      if (code > 0) {
        err = new Error('Consumer process exited with code: ' + code)
      }
      wrappedCallback(err)
    })

    this.consumers[id].send(config.shutdownMessage)
  }

  private _killAllConsumers (callback: (err: any) => void) {
    this.logger.info('Killing all consumers')
    async.each(this.consumerIds, this._killConsumerById.bind(this), callback)
  }

  // Continuously fetch data about the rest of the network.
  private _loopFetchExternalNetwork () {
    this.logger.info('Starting external network fetch loop')

    const fetchThenWait = done => {
      this._fetchExternalNetwork(function (err) {
        if (err) {
          return done(err)
        }
        setTimeout(done, 5000)
      })
    }

    const handleError = err => {
      this._logAndEmitError(err, 'Error fetching external network data')
    }

    async.forever(fetchThenWait, handleError)
  }

  // Fetch data about the rest of the network.
  private _fetchExternalNetwork (callback: (err?: any) => void) {
    this.cluster.fetchAll((err, clusters) => {
      if (err) {
        return callback(err)
      }

      this.externalNetwork = clusters.Items.filter(cluster => {
        return cluster.get('id') !== this.cluster.id
      }).reduce((memo, cluster) => {
        memo[cluster.get('id')] = cluster.get('activeConsumers')
        return memo
      }, {})

      this.logger.debug({externalNetwork: this.externalNetwork}, 'Updated external network')
      this.emit('updateNetwork')
      callback()
    })
  }

  // Continuously publish data about this cluster to the network.
  private _loopReportClusterToNetwork () {
    this.logger.info('Starting report cluster loop')
    const reportThenWait = done => {
      this._reportClusterToNetwork(err => {
        if (err) {
          return done(err)
        }

        setTimeout(done, 1000)
      })
    }

    const handleError = err => {
      this._logAndEmitError(err, 'Error reporting cluster to network')
    }

    async.forever(reportThenWait, handleError)
  }

  // Publish data about this cluster to the nework.
  private _reportClusterToNetwork (callback: (err: any) => void) {
    this.logger.debug({consumers: this.consumerIds.length}, 'Rerpoting cluster to network')
    this.cluster.reportActiveConsumers(this.consumerIds.length, callback)
  }

  // Garbage collect expired clusters from the network.
  private _garbageCollectClusters () {
    if (Date.now() < (this.lastGarbageCollectedAt + (1000 * 60))) {
      return
    }

    this.lastGarbageCollectedAt = Date.now()
    this.cluster.garbageCollect((err, garbageCollectedClusters) => {
      if (err) {
        this.logger.error(err, 'Error garbage collecting clusters, continuing cluster execution anyway')
        return
      }

      if (garbageCollectedClusters.length) {
        this.logger.info('Garbage collected %d clusters', garbageCollectedClusters.length)
      }
    })
  }

  private _logAndEmitError(err: Error, desc?: string) {
    this.logger.error(desc)
    this.logger.error(err)

    // Only start the shutdown process once
    if (this.isShuttingDownFromError) {
      return
    }

    this.isShuttingDownFromError = true

    // Kill all consumers and then emit an error so that the cluster can be re-spawned
    this._killAllConsumers((killErr?: Error) => {
      if (killErr) {
        this.logger.error(killErr)
      }

      // Emit the original error that started the shutdown process
      this.emit('error', err)
    })
  }
}
