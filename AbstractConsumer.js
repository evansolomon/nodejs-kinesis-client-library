var util = require('util')

var async = require('async')
var bunyan = require('bunyan')
var _ = require('underscore')

var Lease = require('./lib/models/Lease')
var awsFactory = require('./lib/aws/factory')
var kinesis = require('./lib/aws/kinesis')

module.exports = AbstractConsumer

function AbstractConsumer(opts) {
  this.client = awsFactory(opts.awsConfig, 'Kinesis')

  this.streamName = opts.streamName
  this.shardId = opts.shardId
  this.leaseCounter = opts.leaseCounter
  this.tableName = opts.tableName
  this.awsConfig = opts.awsConfig

  this.logger = bunyan.createLogger({
    name: 'KinesisConsumer',
    streamName: opts.streamName,
    shardId: opts.shardId
  })
}

AbstractConsumer.prototype.init = function () {
  var _this = this
  this._setupLease()

  async.series([
    _this._reserveLease.bind(_this),
    function (done) {
      _this.lease.getCheckpoint(function (err, checkpoint) {
        if (err) return done(err)

        _this.log({checkpoint: checkpoint}, 'Got starting checkpoint')
        _this.maxSequenceNumber = checkpoint
        _this._updateShardIterator(checkpoint, done)
      })
    },

    _this.initialize.bind(_this)
  ], function (err) {
    if (err) {
      return this._exit(err)
    }

    _this._loopGetRecords()
    _this._loopReserveLease()
  })
}

AbstractConsumer.prototype.log = function () {
  this.logger.info.apply(this.logger, arguments)
}

AbstractConsumer.prototype._loopGetRecords = function () {
  var _this = this
  var maxCallFrequency = 1000

  this.log('Starting getRecords loop')

  async.forever(function (done) {
    var gotRecordsAt = Date.now()

    _this._getRecords(function (err) {
      if (err) return done(err)

      var timeToWait = Math.max(0, maxCallFrequency - (Date.now() - gotRecordsAt))

      if (timeToWait > 0) {
        setTimeout(done, timeToWait)
      } else {
        done()
      }
    })
  }, function (err) {
    _this._exit(err)
  })
}

AbstractConsumer.prototype._loopReserveLease = function () {
  var _this = this

  this.log('Starting reserveLease loop')

  async.forever(function (done) {
    setTimeout(_this._reserveLease.bind(_this, done), 5000)
  }, function (err) {
    _this._exit(err)
  })
}

AbstractConsumer.prototype._setupLease = function () {
  var id = this.shardId
  var leaseCounter = this.leaseCounter || null
  var tableName = this.tableName
  var awsConfig = this.awsConfig

  this.log({leaseCounter: leaseCounter, tableName: tableName}, 'Setting up lease')

  this.lease = new Lease(id, leaseCounter, tableName, awsConfig)
}

AbstractConsumer.prototype._reserveLease = function (callback) {
  this.logger.debug('Reserving lease')
  this.lease.reserve(callback)
}

AbstractConsumer.prototype._markFinished = function () {
  var _this = this
  this.log('Marking shard as finished')

  this.lease.markFinished(function (err) {
    _this._exit(err)
  })
}

AbstractConsumer.prototype._getRecords = function (callback) {
  var _this = this

  kinesis.getRecords(this.client, this.nextShardIterator, function (err, data) {
    // Handle known errors
    if (err && err.code === 'ExpiredIteratorException') {
      _this.log('Shard iterator expired, updating before next getRecords call')
      return _this._updateShardIterator(_this.maxSequenceNumber, function (err) {
        if (err) return callback(err)

        _this._getRecords(callback)
      })
    }

    if (err && err.code === 'ProvisionedThroughputExceededException') {
      _this.log('Provisioned throughput exceeded, pausing before next getRecords call')
      return setTimeout(function () {
        _this._getRecords(callback)
      }, 5000)
    }

    // We have an error but don't know how to handle it
    if (err) return callback(err)

    // Save this in case we need to checkpoint it in a future request before we get more records
    if (data.NextShardIterator != null) {
      _this.nextShardIterator = data.NextShardIterator
    }

    // We have processed all the data from a closed stream
    if (data.NextShardIterator == null && data.Records.length === 0) {
      return _this.markFinished()
    }

    var lastSequenceNumber = _.pluck(data.Records, 'SequenceNumber').pop()
    _this.maxSequenceNumber = lastSequenceNumber || _this.maxSequenceNumber

    _this._processRecords(data.Records, callback)
  })
}

AbstractConsumer.prototype._processRecords = function (records, callback) {
  var _this = this
  this.processRecords(records, function (err, checkpointSequenceNumber) {
    if (err) return callback(err)

    // Don't checkpoint
    if (! checkpointSequenceNumber) return callback()
    // We haven't actually gotten any records so there is nothing to checkpoint
    if (! _this.maxSequenceNumber) return callback()

    // Default case to checkpoint the latest sequence number
    if (checkpointSequenceNumber === true) {
      checkpointSequenceNumber = _this.maxSequenceNumber
    }

    _this.lease.checkpoint(checkpointSequenceNumber, callback)
  })
}

AbstractConsumer.prototype.initialize = function (callback) {
  this.log('No initialize method defined, skipping')
  callback()
}

AbstractConsumer.prototype.processRecords = function () {
  throw new Error('processRecords must be defined by the consumer class')
}

AbstractConsumer.prototype.shutdown = function (callback) {
  this.log('No shutdown method defined, skipping')
  callback()
}

AbstractConsumer.prototype._updateShardIterator = function (sequenceNumber, callback) {
  var _this = this
  var type = sequenceNumber ? 'AFTER_SEQUENCE_NUMBER' : 'TRIM_HORIZON'

  this.log({iteratorType: type, sequenceNumber: sequenceNumber}, 'Updating shard iterator')
  kinesis.getShardIterator(this.client, this.streamName, this.shardId, type, sequenceNumber, function (err, data) {
    if (err) return callback(err)

    _this.log(data, 'Updated shard iterator')
    _this.nextShardIterator = data.ShardIterator
    callback()
  })
}

AbstractConsumer.prototype._exit = function (err) {
  var _this = this
  if (err) {
    this.logger.error(err)
  }

  setTimeout(function () {
    _this.logger.error('Forcing exit based on shutdown timeout')
    // Exiting with 1 because the shutdown process took too long
    process.exit(1)
  }, 30000)

  this.log('Starting shutdown')
  this.shutdown(function () {
    var exitCode = err == null ? 0 : 1
    process.exit(exitCode)
  })
}



AbstractConsumer.extend = function (args) {
  var opts = JSON.parse(process.env.CONSUMER_INSTANCE_OPTS)
  function Ctor() {
    AbstractConsumer.call(this, opts)
  }
  util.inherits(Ctor, AbstractConsumer)

  var methods = ['processRecords', 'initialize', 'shutdown']
  methods.forEach(function (method) {
    if (! args[method]) return
    Ctor.prototype[method] = args[method]
  })

  var consumer = new Ctor()
  consumer.init()
}
