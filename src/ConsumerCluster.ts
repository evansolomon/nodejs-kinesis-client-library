import {EventEmitter} from 'events'
import {Worker, fork, setupMaster} from 'cluster'
import {join} from 'path'
import {format as formatUrl} from 'url'

import {difference, pluck, without} from 'underscore'
import {auto, parallel, each, forever} from 'async'
import {ClientConfig, Kinesis} from 'aws-sdk'
import {Logger, createLogger} from 'bunyan'
import {Queries} from 'vogels'

import {createKinesisClient} from './lib/aws/factory'
import config from './lib/config'
import {listShards} from './lib/aws/kinesis'
import {Lease} from './lib/models/Lease'
import {Cluster, Capacity as ClusterCapacity} from './lib/models/Cluster'
import {Stream} from './lib/models/Stream'
import {create as createServer} from './lib/server'


interface ClusterWorkerWithOpts extends Worker {
  opts: { shardId: string }
}

interface AWSEndpoints {
  kinesis: string
  dynamo: string
}

export interface ConsumerClusterOpts {
  streamName: string
  tableName: string
  awsConfig: ClientConfig
  dynamoEndpoint?: string
  localDynamo: Boolean
  kinesisEndpoint?: string
  localKinesis: Boolean
  localKinesisPort?: string
  capacity: ClusterCapacity
  startingIteratorType?: string
  logLevel?: string
  numRecords?: number
  timeBetweenReads?: number
}


// Cluster of consumers.
export class ConsumerCluster extends EventEmitter {
  public cluster: Cluster
  private opts: ConsumerClusterOpts
  private logger: Logger
  private kinesis: Kinesis
  private isShuttingDownFromError = false
  private externalNetwork = {}
  private consumers = {}
  private consumerIds = []
  private lastGarbageCollectedAt = Date.now()
  private endpoints: AWSEndpoints

  constructor(pathToConsumer: string, opts: ConsumerClusterOpts) {
    super()
    this.opts = opts

    this.logger = createLogger({
      name: 'KinesisCluster',
      level: opts.logLevel,
    })

    setupMaster({
      exec: pathToConsumer,
      silent: true,
    })

    this.endpoints = {
      kinesis: this.getKinesisEndpoint(),
      dynamo: this.getDynamoEndpoint(),
    }

    this.kinesis = createKinesisClient(this.opts.awsConfig, this.endpoints.kinesis)
    this.cluster = new Cluster(this.opts.tableName, this.opts.awsConfig, this.endpoints.dynamo)
    this.init()
  }

  private init() {
    auto({
      tableExists: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        Cluster.tableExists(tableName, awsConfig, this.getDynamoEndpoint(), done)
      },

      createTable: ['tableExists', (done, data) => {
        if (data.tableExists) {
          return done()
        }

        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        const capacity = this.opts.capacity || {}

        this.logger.info({ table: tableName }, 'Creating DynamoDB table')
        Cluster.createTable(tableName, awsConfig, capacity, this.getDynamoEndpoint(), done)
      }],

      createStream: done => {
        const streamName = this.opts.streamName
        const streamModel = new Stream(streamName, this.kinesis)

        streamModel.exists((err, exists) => {
          if (err) {
            return done(err)
          }

          if (exists) {
            return done()
          }

          this.kinesis.createStream({ StreamName: streamName, ShardCount: 1 }, err => {
            if (err) {
              return done(err)
            }

            streamModel.onActive(done)
          })
        })
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error ensuring Dynamo table exists')
      }

      this.bindListeners()
      this.loopReportClusterToNetwork()
      this.loopFetchExternalNetwork()
    })
  }

  private getKinesisEndpoint() {
    const isLocal = this.opts.localKinesis
    const port = this.opts.localKinesisPort
    const customEndpoint = this.opts.kinesisEndpoint
    let endpoint = null

    if (isLocal) {
      const endpointConfig = config.localKinesisEndpoint
      if (port) {
        endpointConfig.port = port
      }
      endpoint = formatUrl(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  private getDynamoEndpoint() {
    const isLocal = this.opts.localDynamo
    const customEndpoint = this.opts.dynamoEndpoint
    let endpoint = null

    if (isLocal) {
      var endpointConfig = config.localDynamoDBEndpoint
      endpoint = formatUrl(endpointConfig)
    } else if (customEndpoint) {
      endpoint = customEndpoint
    }

    return endpoint
  }

  // Run an HTTP server. Useful as a health check.
  public serveHttp(port: string | number) {
    this.logger.debug('Starting HTTP server on port %s', port)
    createServer(port, () => this.consumerIds.length)
  }

  private bindListeners() {
    this.on('updateNetwork', () => {
      this.garbageCollectClusters()

      if (this.shouldTryToAcquireMoreShards()) {
        this.logger.debug('Should try to acquire more shards')
        this.fetchAvailableShard()
      } else if (this.hasTooManyShards()) {
        this.logger.debug({ consumerIds: this.consumerIds }, 'Have too many shards')
        this.killConsumer(err => {
          if (err) {
            this.logAndEmitError(err)
          }
        })
      }
    })

    this.on('availableShard', (shardId, leaseCounter) => {
      // Stops accepting consumers, since the cluster will be reset based one an error
      if (this.isShuttingDownFromError) {
        return
      }

      this.spawn(shardId, leaseCounter)
    })

  }

  // Compare cluster state to external network to figure out if we should try to change our shard allocation.
  private shouldTryToAcquireMoreShards() {
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
  private hasTooManyShards() {
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
  private fetchAvailableShard() {
    // Hack around typescript
    var _asyncResults = <{ allShardIds: string[]; leases: Queries.Query.Result; }>{}

    parallel({
      allShardIds: done => {
        listShards(this.kinesis, this.opts.streamName, (err, shards) => {
          if (err) {
            return done(err)
          }

          const shardIds = pluck(shards, 'ShardId')
          _asyncResults.allShardIds = shardIds
          done()
        })
      },
      leases: done => {
        const tableName = this.opts.tableName
        const awsConfig = this.opts.awsConfig
        Lease.fetchAll(tableName, awsConfig, this.getDynamoEndpoint(), (err, leases) => {
          if (err) {
            return done(err)
          }

          _asyncResults.leases = leases
          done()
        })
      },
    }, err => {
      if (err) {
        return this.logAndEmitError(err, 'Error fetching available shards')
      }

      const allShardIds = _asyncResults.allShardIds
      const leases = _asyncResults.leases
      const leaseItems = leases.Items

      const finishedShardIds = leaseItems.filter(lease => {
        return lease.get('isFinished')
      }).map(lease => {
        return <string>lease.get('id')
      })

      const allUnfinishedShardIds = allShardIds.filter(id => {
        return finishedShardIds.indexOf(id) === -1
      })

      const leasedShardIds = leaseItems.map(item => {
        return item.get('id')
      })
      const newShardIds = difference(allUnfinishedShardIds, leasedShardIds)

      // If there are shards theat have not been leased, pick one
      if (newShardIds.length > 0) {
        this.logger.info({ newShardIds: newShardIds }, 'Unleased shards available')
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
        this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Found available shard')
        return this.emit('availableShard', shardId, leaseCounter)
      }
    })
  }

  // Create a new consumer processes.
  private spawn(shardId: string, leaseCounter: number) {
    this.logger.info({ shardId: shardId, leaseCounter: leaseCounter }, 'Spawning consumer')
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
      logLevel: this.opts.logLevel,
    }

    const env = {
      CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
      CONSUMER_SUPER_CLASS_PATH: join(__dirname, 'AbstractConsumer.js'),
    }

    const consumer = <ClusterWorkerWithOpts>fork(env)
    consumer.opts = consumerOpts
    consumer.process.stdout.pipe(process.stdout)
    consumer.process.stderr.pipe(process.stderr)
    this.addConsumer(consumer)
  }

  // Add a consumer to the cluster.
  private addConsumer(consumer: ClusterWorkerWithOpts) {
    this.consumerIds.push(consumer.id)
    this.consumers[consumer.id] = consumer

    consumer.once('exit', code => {
      const logMethod = code === 0 ? 'info' : 'error'
      this.logger[logMethod]({ shardId: consumer.opts.shardId, exitCode: code }, 'Consumer exited')

      this.consumerIds = without(this.consumerIds, consumer.id)
      delete this.consumers[consumer.id]
    })
  }

  // Kill any consumer in the cluster.
  private killConsumer(callback: (err: any) => void) {
    const id = this.consumerIds[0]
    this.killConsumerById(id, callback)
  }

  // Kill a specific consumer in the cluster.
  private killConsumerById(id: number, callback: (err: any) => void) {
    this.logger.info({ id: id }, 'Killing consumer')

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

  private killAllConsumers(callback: (err: any) => void) {
    this.logger.info('Killing all consumers')
    each(this.consumerIds, this.killConsumerById.bind(this), callback)
  }

  // Continuously fetch data about the rest of the network.
  private loopFetchExternalNetwork() {
    this.logger.info('Starting external network fetch loop')

    const fetchThenWait = done => {
      this.fetchExternalNetwork(function(err) {
        if (err) {
          return done(err)
        }
        setTimeout(done, 5000)
      })
    }

    const handleError = err => {
      this.logAndEmitError(err, 'Error fetching external network data')
    }

    forever(fetchThenWait, handleError)
  }

  // Fetch data about the rest of the network.
  private fetchExternalNetwork(callback: (err?: any) => void) {
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

      this.logger.debug({ externalNetwork: this.externalNetwork }, 'Updated external network')
      this.emit('updateNetwork')
      callback()
    })
  }

  // Continuously publish data about this cluster to the network.
  private loopReportClusterToNetwork() {
    this.logger.info('Starting report cluster loop')
    const reportThenWait = done => {
      this.reportClusterToNetwork(err => {
        if (err) {
          return done(err)
        }

        setTimeout(done, 1000)
      })
    }

    const handleError = err => {
      this.logAndEmitError(err, 'Error reporting cluster to network')
    }

    forever(reportThenWait, handleError)
  }

  // Publish data about this cluster to the nework.
  private reportClusterToNetwork(callback: (err: any) => void) {
    this.logger.debug({ consumers: this.consumerIds.length }, 'Rerpoting cluster to network')
    this.cluster.reportActiveConsumers(this.consumerIds.length, callback)
  }

  // Garbage collect expired clusters from the network.
  private garbageCollectClusters() {
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

  private logAndEmitError(err: Error, desc?: string) {
    this.logger.error(desc)
    this.logger.error(err)

    // Only start the shutdown process once
    if (this.isShuttingDownFromError) {
      return
    }

    this.isShuttingDownFromError = true

    // Kill all consumers and then emit an error so that the cluster can be re-spawned
    this.killAllConsumers((killErr?: Error) => {
      if (killErr) {
        this.logger.error(killErr)
      }

      // Emit the original error that started the shutdown process
      this.emit('error', err)
    })
  }
}
