import events = require('events')
import nodeCluster = require('cluster')
import path = require('path')
import url = require('url')

import _ = require('underscore')
import AWS = require('aws-sdk')
import async = require('async')
import bunyan = require('bunyan')
import vogels = require('vogels')

import awsFactory = require('./lib/aws/factory')
import config = require('./lib/config')
import kinesis = require('./lib/aws/kinesis')
import lease = require('./lib/models/Lease')
import cluster = require('./lib/models/Cluster')
import stream = require('./lib/models/Stream')
import server = require('./lib/server')


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
    var _this = this

    async.auto({
      tableExists: function (done) {
        var tableName = _this.opts.tableName
        var awsConfig = _this.opts.awsConfig
        cluster.Model.tableExists(tableName, awsConfig, _this.getDynamoEndpoint(), done)
      },

      createTable: ['tableExists', function (done, data) {
        if (data.tableExists) {
          return done()
        }

        var tableName = _this.opts.tableName
        var awsConfig = _this.opts.awsConfig
        var capacity = _this.opts.capacity || {}

        _this.logger.info({table: tableName}, 'Creating DynamoDB table')
        cluster.Model.createTable(tableName, awsConfig, capacity, _this.getDynamoEndpoint(), done)
      }],

      createStream: function (done) {
        var streamName = _this.opts.streamName
        var streamModel = new stream.Stream(streamName, _this.kinesis)

        streamModel.exists(function (err, exists) {
          if (err) {
            return done(err)
          }
          if (exists) {
            return done()
          }

          _this.kinesis.createStream({StreamName: streamName, ShardCount: 1}, function (err) {
            if (err) {
              return done(err)
            }
            streamModel.onActive(done)
          })
        })
      }
    }, function (err) {
      if (err) {
        return _this._logAndEmitError(err, 'Error ensuring Dynamo table exists')
      }

      _this._bindListeners()
      _this._loopReportClusterToNetwork()
      _this._loopFetchExternalNetwork()
    })
  }

  private getKinesisEndpoint () {
    var isLocal = this.opts.localKinesis
    var port = this.opts.localKinesisPort
    var customEndpoint = this.opts.kinesisEndpoint
    var endpoint = null
    if (isLocal) {
      var endpointConfig = config.localKinesisEndpoint
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
    var isLocal = this.opts.localDynamo
    var customEndpoint = this.opts.dynamoEndpoint
    var endpoint = null
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
    server.create(port, function () {
      return this.consumerIds.length
    }.bind(this))
  }

  private _bindListeners () {
    var _this = this

    this.on('updateNetwork', function () {
      _this._garbageCollectClusters()

      if (_this._shouldTryToAcquireMoreShards()) {
        _this.logger.debug('Should try to acquire more shards')
        _this._fetchAvailableShard()
      } else if (_this._hasTooManyShards()) {
        _this.logger.debug({consumerIds: _this.consumerIds}, 'Have too many shards')
        _this._killConsumer(function (err) {
          if (err) {
            _this._logAndEmitError(err)
          }
        })
      }
    })

    this.on('availableShard', function (shardId, leaseCounter) {
      // Stops accepting consumers, since the cluster will be reset based one an error
      if (_this.isShuttingDownFromError) {
        return
      }

      _this._spawn(shardId, leaseCounter)
    })

  }

  // Compare cluster state to external network to figure out if we should try to change our shard allocation.
  private _shouldTryToAcquireMoreShards () {
    if (this.consumerIds.length === 0) {
      return true
    }

    var externalNetwork = this.externalNetwork
    var networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return true
    }

    var lowestInOutterNetwork = networkKeys.reduce(function (memo, key) {
      var count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length <= lowestInOutterNetwork
  }

  // Determine if we have too many shards compared to the rest of the network.
  private _hasTooManyShards () {
    var externalNetwork = this.externalNetwork

    var networkKeys = Object.keys(externalNetwork)
    if (networkKeys.length === 0) {
      return false
    }

    var lowestInOutterNetwork = networkKeys.reduce(function (memo, key) {
      var count = externalNetwork[key]
      if (count < memo) {
        memo = count
      }

      return memo
    }, Infinity)

    return this.consumerIds.length > (lowestInOutterNetwork + 1)
  }

  // Fetch data about unleased shards.
  private _fetchAvailableShard () {
    var _this = this
    // Hack around typescript
    var _asyncResults = <{allShardIds: string[]; leases: vogels.Queries.Query.Result;}> {}

    async.parallel({
      allShardIds: function (done) {
        kinesis.listShards(_this.kinesis, _this.opts.streamName, function (err, shards) {
          if (err) {
            return done(err)
          }

          var shardIds = _.pluck(shards, 'ShardId')
          _asyncResults.allShardIds = shardIds
          done()
        })
      },
      leases: function (done) {
        var tableName = _this.opts.tableName
        var awsConfig = _this.opts.awsConfig
        lease.Model.fetchAll(tableName, awsConfig, _this.getDynamoEndpoint(), function (err, leases) {
          if (err) {
            return done(err)
          }

          _asyncResults.leases = leases
          done()
        })
      }
    }, function (err) {
      if (err) {
        return _this._logAndEmitError(err, 'Error fetching available shards')
      }

      var allShardIds = _asyncResults.allShardIds
      var leases = _asyncResults.leases
      var leaseItems = leases.Items

      var finishedShardIds = leaseItems.filter(function (lease) {
        return lease.get('isFinished')
      }).map(function (lease) {
        return <string> lease.get('id')
      })

      var allUnfinishedShardIds = allShardIds.filter(function (id) {
        return finishedShardIds.indexOf(id) === -1
      })

      var leasedShardIds = leaseItems.map(function (item) {
        return item.get('id')
      })
      var newShardIds = _.difference(allUnfinishedShardIds, leasedShardIds)

      // If there are shards theat have not been leased, pick one
      if (newShardIds.length > 0) {
        _this.logger.info({newShardIds: newShardIds}, 'Unleased shards available')
        return _this.emit('availableShard', newShardIds[0], null)
      }

      // Try to find the first expired lease
      var currentLease
      for (var i = 0; i < leaseItems.length; i++) {
        currentLease = leaseItems[i]
        if (currentLease.get('expiresAt') > Date.now()) {
          continue
        }
        if (currentLease.get('isFinished')) {
          continue
        }

        var shardId = currentLease.get('id')
        var leaseCounter = currentLease.get('leaseCounter')
        _this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Found available shard')
        return _this.emit('availableShard', shardId, leaseCounter)
      }
    })
  }

  // Create a new consumer processes.
  private _spawn (shardId: string, leaseCounter: number) {
    this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Spawning consumer')
    var consumerOpts = {
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

    var env = {
      CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
      CONSUMER_SUPER_CLASS_PATH: path.join(__dirname, 'AbstractConsumer.js')
    }

    var consumer = <ClusterWorkerWithOpts> nodeCluster.fork(env)
    consumer.opts = consumerOpts
    consumer.process.stdout.pipe(process.stdout)
    consumer.process.stderr.pipe(process.stderr)
    this._addConsumer(consumer)
  }

  // Add a consumer to the cluster.
  private _addConsumer (consumer: ClusterWorkerWithOpts) {
    this.consumerIds.push(consumer.id)
    this.consumers[consumer.id] = consumer

    consumer.once('exit', function (code) {
      var logMethod = code === 0 ? 'info' : 'error'
      this.logger[logMethod]({shardId: consumer.opts.shardId, exitCode: code}, 'Consumer exited')

      this.consumerIds = _.without(this.consumerIds, consumer.id)
      delete this.consumers[consumer.id]
    }.bind(this))
  }

  // Kill any consumer in the cluster.
  private _killConsumer (callback: (err: any) => void) {
    var id = this.consumerIds[0]
    this._killConsumerById(id, callback)
  }

  // Kill a specific consumer in the cluster.
  private _killConsumerById (id: number, callback: (err: any) => void) {
    this.logger.info({id: id}, 'Killing consumer')

    var callbackWasCalled = false
    var wrappedCallback = function (err: any) {
      if (callbackWasCalled) {
        return
      }
      callbackWasCalled = true
      callback(err)
    }

    // Force kill the consumer in 40 seconds, giving enough time for the consumer's shutdown
    // process to finish
    var timer = setTimeout(function () {
      if (this.consumers[id]) {
        this.consumers[id].kill()
      }
      wrappedCallback(new Error('Consumer did not exit in time'))
    }.bind(this), 40000)

    this.consumers[id].once('exit', function (code) {
      clearTimeout(timer)
      var err = null
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
    var _this = this
    this.logger.info('Starting external network fetch loop')

    function fetchThenWait(done) {
      _this._fetchExternalNetwork(function (err) {
        if (err) {
          return done(err)
        }
        setTimeout(done, 5000)
      })
    }

    function handleError(err) {
      _this._logAndEmitError(err, 'Error fetching external network data')
    }

    async.forever(fetchThenWait, handleError)
  }

  // Fetch data about the rest of the network.
  private _fetchExternalNetwork (callback: (err?: any) => void) {
    var _this = this

    this.cluster.fetchAll(function (err, clusters) {
      if (err) {
        return callback(err)
      }

      _this.externalNetwork = clusters.Items.filter(function (cluster) {
        return cluster.get('id') !== _this.cluster.id
      }).reduce(function (memo, cluster) {
        memo[cluster.get('id')] = cluster.get('activeConsumers')
        return memo
      }, {})

      _this.logger.debug({externalNetwork: _this.externalNetwork}, 'Updated external network')
      _this.emit('updateNetwork')
      callback()
    })
  }

  // Continuously publish data about this cluster to the network.
  private _loopReportClusterToNetwork () {
    var _this = this
    this.logger.info('Starting report cluster loop')
    function reportThenWait(done) {
      _this._reportClusterToNetwork(function (err) {
        if (err) {
          return done(err)
        }

        setTimeout(done, 1000)
      })
    }

    function handleError(err) {
      _this._logAndEmitError(err, 'Error reporting cluster to network')
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
    this.cluster.garbageCollect(function (err, garbageCollectedClusters) {
      if (err) {
        this.logger.error(err, 'Error garbage collecting clusters, continuing cluster execution anyway')
        return
      }

      if (garbageCollectedClusters.length) {
        this.logger.info('Garbage collected %d clusters', garbageCollectedClusters.length)
      }
    }.bind(this))
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
    this._killAllConsumers(function (killErr?: Error) {
      if (killErr) {
        this.logger.error(killErr)
      }

      // Emit the original error that started the shutdown process
      this.emit('error', err)
    }.bind(this))
  }
}
