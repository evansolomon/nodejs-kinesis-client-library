var path = require('path')
var EventEmitter = require('events').EventEmitter
var util = require('util')
var nodeCluster = require('cluster')

var async = require('async')
var _ = require('underscore')
var bunyan = require('bunyan')

var server = require('./lib/server')
var ClusterModel = require('./lib/models/Cluster')
var LeaseModel = require('./lib/models/Lease')
var awsFactory = require('./lib/aws/factory')
var kinesis = require('./lib/aws/kinesis')

module.exports = ConsumerCluster

function ConsumerCluster(pathToConsumer, opts) {
  EventEmitter.call(this)

  this.opts = opts
  this.logger = bunyan.createLogger({name: 'KinesisCluster'})

  nodeCluster.setupMaster({
    exec: pathToConsumer,
    silent: true
  })

  this.cluster = new ClusterModel(opts.tableName, opts.awsConfig)

  this.client = awsFactory(opts.awsConfig, 'Kinesis')

  this.externalNetwork = {}
  this.consumers = {}
  this.consumerIds = []

  this.lastGarbageCollectedAt = Date.now()
}
util.inherits(ConsumerCluster, EventEmitter)

ConsumerCluster.prototype.init = function () {
  var _this = this

  async.auto({
    tableExists: function (done) {
      ClusterModel.tableExists(_this.opts.tableName, _this.opts.awsConfig, done)
    },

    createTable: ['tableExists', function (done, data) {
      if (data.tableExists) return done()
      _this.logger.info({table: _this.opts.tableName}, 'Creating DynamoDB table')
      ClusterModel.createTable(_this.opts.tableName, _this.opts.awsConfig, done)
    }]
  }, function (err) {
    if (err) return logAndEmitError(err, 'Error ensuring Dynamo table exists')

    _this._bindListeners()
    _this._loopReportClusterToNetwork()
    _this._loopFetchExternalNetwork()
  })
}

ConsumerCluster.prototype.serveHttp = function (port) {
  this.logger.debug('Starting HTTP server on port %s', port)
  server.create(port, function () {
    return this.consumerIds.length
  }.bind(this))
}

ConsumerCluster.prototype._bindListeners = function () {
  var _this = this

  this.on('updateNetwork', function () {
    _this._garbageCollectClusters()

    if (_this._shouldTryToAcquireMoreShards()) {
      _this.logger.debug('Should try to acquire more shards')
      _this.fetchAvailableShard()
    } else if (_this._hasTooManyShards()) {
      _this.logger.debug({consumerIds: _this.consumerIds}, 'Have too many shards')
      _this._killConsumer()
    }
  })

  this.on('availableShard', function (shardId, leaseCounter) {
    _this.spawn(shardId, leaseCounter)
  })
}

/**
 * Compare cluster state to external network
 */
ConsumerCluster.prototype._shouldTryToAcquireMoreShards = function () {
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

ConsumerCluster.prototype._hasTooManyShards = function () {
  var externalNetwork = this.externalNetwork

  var networkKeys = Object.keys(externalNetwork)
  if (networkKeys.length === 0) return false

  var lowestInOutterNetwork = networkKeys.reduce(function (memo, key) {
    var count = externalNetwork[key]
    if (count < memo) {
      memo = count
    }

    return memo
  }, Infinity)

  return this.consumerIds.length > (lowestInOutterNetwork + 1)
}


/**
 * Get data about unleased shards
 */

ConsumerCluster.prototype.fetchAvailableShard = function () {
  var _this = this
  async.parallel({
    allShardIds: function (done) {
      kinesis.listShards(_this.client, _this.opts.streamName, function (err, shards) {
        if (err) return done(err)

        var shardIds = _.pluck(shards, 'ShardId')
        done(null, shardIds)
      })
    },
    leases: function (done) {
      LeaseModel.fetchAll(_this.opts.tableName, _this.opts.awsConfig, function (err, leases) {
        if (err) return done(err)
        done(null, leases.Items)
      })
    }
  }, function (err, data) {
    if (err) {
      return _this.logAndEmitError(err, 'Error fetching available shards')
    }

    var finishedShardIds = data.leases.filter(function (lease) {
      return lease.get('isFinished')
    })

    var allUnfinishedShardIds = data.allShardIds.filter(function (id) {
      return finishedShardIds.indexOf(id) === -1
    })

    var leasedShardIds = data.leases.map(function (item) {
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
    for (var i = 0; i < data.leases.length; i++) {
      currentLease = data.leases[i]
      if (currentLease.get('expiresAt') > Date.now()) continue
      if (currentLease.get('isFinished')) continue

      var shardId = currentLease.get('id')
      var leaseCounter = currentLease.get('leaseCounter')
      _this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Found available shard')
      return _this.emit('availableShard', shardId, leaseCounter)
    }
  })
}


/**
 * Create and manage consumer processes
 */

ConsumerCluster.prototype.spawn = function (shardId, leaseCounter) {
  if (! shardId) {
    throw new Error('Cannot spawn consumer without shard ID')
  }

  this.logger.info({shardId: shardId, leaseCounter: leaseCounter}, 'Spawning consumer')
  var consumerOpts = {
    tableName: this.opts.tableName,
    awsConfig: this.opts.awsConfig,
    streamName: this.opts.streamName,
    shardId: shardId,
    leaseCounter: leaseCounter
  }

  var env = {
    CONSUMER_INSTANCE_OPTS: JSON.stringify(consumerOpts),
    CONSUMER_SUPER_CLASS_PATH: path.join(__dirname, 'AbstractConsumer.js')
  }

  var consumer = nodeCluster.fork(env)
  consumer.opts = consumerOpts
  consumer.process.stdout.pipe(process.stdout)
  consumer.process.stderr.pipe(process.stderr)
  this._addConsumer(consumer)
}

ConsumerCluster.prototype._addConsumer = function (consumer) {
  this.consumerIds.push(consumer.id)
  this.consumers[consumer.id] = consumer

  consumer.once('exit', function () {
    this.logger.info({shardId: consumer.opts.shardId}, 'Consumer exited')
    this.consumerIds = _.without(this.consumerIds, consumer.id)
    delete this.consumers[consumer.id]
  }.bind(this))
}

ConsumerCluster.prototype._killConsumer = function () {
  var id = this.consumerIds[0]
  this.logger.info({id: id}, 'Killing consumer')
  this.consumers[id].kill()
}


/**
 * Keep external network state up to date in this cluster
 */
ConsumerCluster.prototype._loopFetchExternalNetwork = function () {
  var _this = this
  this.logger.info('Starting external network fetch loop')

  function fetchThenWait(done) {
    _this._fetchExternalNetwork(function (err) {
      if (err) return done(err)
      setTimeout(done, 5000)
    })
  }

  function handleError(err) {
    _this.logAndEmitError(err, 'Error fetching external network data')
  }

  async.forever(fetchThenWait, handleError)
}

ConsumerCluster.prototype._fetchExternalNetwork = function (callback) {
  var _this = this

  this.cluster.fetchAll(function (err, clusters) {
    if (err) return callback(err)

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


/**
 * Keep cluster state up to date in network DB
 */
ConsumerCluster.prototype._loopReportClusterToNetwork = function () {
  var _this = this
  this.logger.info('Starting report cluster loop')
  function reportThenWait(done) {
    _this._reportClusterToNetwork(function (err) {
      if (err) return done(err)
      setTimeout(done, 1000)
    })
  }

  function handleError(err) {
    _this.logAndEmitError(err, 'Error reporting cluster to network')
  }

  async.forever(reportThenWait, handleError)
}

ConsumerCluster.prototype._reportClusterToNetwork = function (callback) {
  this.logger.debug({consumers: this.consumerIds.length}, 'Rerpoting cluster to network')
  this.cluster.reportActiveConsumers(this.consumerIds.length, callback)
}

ConsumerCluster.prototype._garbageCollectClusters = function () {
  if (Date.now() < (this.lastGarbageCollectedAt + (1000 * 60))) return

  this.logger.info('Garbage collecting clusters')
  this.lastGarbageCollectedAt = Date.now()
  this.cluster.garbageCollect(function (err) {
    if (! err) return
    console.error('Error garbage collecting clusters, continuing cluster execution anyway')
    console.error(err.stack)
  })
}

ConsumerCluster.prototype.logAndEmitError = function (err, desc) {
  this.logger.error(desc)
  this.logger.error(err)

  this.emit('error', err)
}
