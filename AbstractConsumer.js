/**
 * @fileoverview Extendable stream consumer.
 */

var util = require('util')

var _ = require('underscore')
var async = require('async')
var bunyan = require('bunyan')

var aws = require('./lib/aws/factory')
var config = require('./lib/config')
var kinesis = require('./lib/aws/kinesis')
var models = require('./lib/models')

module.exports = AbstractConsumer

/**
 * Stream consumer, meant to be extended.
 *
 * @param {{
 *   streamName: string,
 *   shardId: string,
 *   leaseCounter: number=,
 *   tableName: string,
 *   awsConfig: Object=,
 *   startingIteratorType: string=
 * }} opts
 */
function AbstractConsumer(opts) {
  this.client = aws.create(opts.awsConfig, false, 'Kinesis')

  this.streamName = opts.streamName
  this.shardId = opts.shardId
  this.leaseCounter = opts.leaseCounter
  this.tableName = opts.tableName
  this.awsConfig = opts.awsConfig
  this.localDynamo = opts.localDynamo

  if (! opts.shardId) {
    return this._exit(new Error('Cannot spawn a consumer without a shard ID'))
  }

  this.startingIteratorType = AbstractConsumer.ShardIteratorTypes[opts.startingIteratorType]
  if (! this.startingIteratorType) {
     this.startingIteratorType = AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE
  }

  this.hasStartedExit = false

  process.on('message', function (msg) {
    if (msg === config.shutdownMessage) {
      this._exit()
    }
  }.bind(this))

  this.logger = bunyan.createLogger({
    name: 'KinesisConsumer',
    streamName: opts.streamName,
    shardId: opts.shardId
  })
}

/**
 * @enum {string}
 */
AbstractConsumer.ShardIteratorTypes = {
  AT_SEQUENCE_NUMBER: 'AT_SEQUENCE_NUMBER',
  AFTER_SEQUENCE_NUMBER: 'AFTER_SEQUENCE_NUMBER',
  TRIM_HORIZON: 'TRIM_HORIZON',
  LATEST: 'LATEST'
}

/**
 * @type {string}
 * @const
 */
AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE = AbstractConsumer.ShardIteratorTypes.TRIM_HORIZON

/**
 * Setup initial consumer state.
 */
AbstractConsumer.prototype.init = function () {
  var _this = this
  this._setupLease()

  async.series([
    _this.initialize.bind(_this),
    _this._reserveLease.bind(_this),
    function (done) {
      _this.lease.getCheckpoint(function (err, checkpoint) {
        if (err) return done(err)

        _this.log({checkpoint: checkpoint}, 'Got starting checkpoint')
        _this.maxSequenceNumber = checkpoint
        _this._updateShardIterator(checkpoint, done)
      })
    }
  ], function (err) {
    if (err) {
      return _this._exit(err)
    }

    _this._loopGetRecords()
    _this._loopReserveLease()
  })
}

/**
 * Log helper.
 */
AbstractConsumer.prototype.log = function () {
  this.logger.info.apply(this.logger, arguments)
}

/**
 * Continuously fetch records from the stream.
 */
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

/**
 * Continuously update this consumer's lease reservation.
 */
AbstractConsumer.prototype._loopReserveLease = function () {
  var _this = this

  this.log('Starting reserveLease loop')

  async.forever(function (done) {
    setTimeout(_this._reserveLease.bind(_this, done), 5000)
  }, function (err) {
    _this._exit(err)
  })
}

/**
 * Setup the initial lease reservation state.
 * @return {[type]} [description]
 */
AbstractConsumer.prototype._setupLease = function () {
  var id = this.shardId
  var leaseCounter = this.leaseCounter || null
  var tableName = this.tableName
  var awsConfig = this.awsConfig
  var localDynamo = !! this.localDynamo

  this.log({leaseCounter: leaseCounter, tableName: tableName}, 'Setting up lease')

  this.lease = new models.Lease(id, leaseCounter, tableName, awsConfig, localDynamo)
}

/**
 * Update the lease in the network database.
 * @param {Function}  callback
 */
AbstractConsumer.prototype._reserveLease = function (callback) {
  this.logger.debug('Reserving lease')
  this.lease.reserve(callback)
}

/**
 * Mark the consumer's shard as finished, then exit.
 */
AbstractConsumer.prototype._markFinished = function () {
  var _this = this
  this.log('Marking shard as finished')

  this.lease.markFinished(function (err) {
    _this._exit(err)
  })
}

/**
 * Get records from the stream and wait for them to be processed.
 * @param {Function} callback
 */
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
      return _this._markFinished()
    }

    var lastSequenceNumber = _.pluck(data.Records, 'SequenceNumber').pop()
    _this.maxSequenceNumber = lastSequenceNumber || _this.maxSequenceNumber

    _this._processRecords(data.Records, callback)
  })
}

/**
 * Wrap the child's processRecords method to handle checkpointing.
 *
 * @param {Array}     records
 * @param {Function}  callback
 */
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

/**
 * Initialize the consumer, called before record processing starts. This method may be
 * implemented by the child. If it is implemented, the callback must be called for
 * processing to begin.
 *
 * @abstract
 * @param {Function}  callback
 */
AbstractConsumer.prototype.initialize = function (callback) {
  this.log('No initialize method defined, skipping')
  callback()
}

/**
 * @callback processRecordsCallback
 * @param {Error=} error
 * @param {Array.<{{
 *   Data: Buffer,
 *   PartitionKey: string,
 *   SequenceNumber: string
 * }}>} records
 */

/**
 * Process a batch of records. This method must be implemented by the child.
 *
 * @abstract
 * @param {processRecordsCallback} callback
 */
AbstractConsumer.prototype.processRecords = function (callback) {
  throw new Error('processRecords must be defined by the consumer class')
}

/**
 * Shutdown the consumer, called before a consumer exits. This method may be implemented
 * by the child. If it is implemented the callback must be called without 30 seconds or
 * the process will force exit.
 *
 * @abstract
 * @param {Function}  callback
 */
AbstractConsumer.prototype.shutdown = function (callback) {
  this.log('No shutdown method defined, skipping')
  callback()
}

/**
 * Get a new shard iterator from Kinesis.
 *
 * @param {string=}   sequenceNumber
 * @param {Function}  callback
 */
AbstractConsumer.prototype._updateShardIterator = function (sequenceNumber, callback) {
  var _this = this
  var type
  if (sequenceNumber) {
    type = AbstractConsumer.ShardIteratorTypes.AFTER_SEQUENCE_NUMBER
  } else {
    type = this.startingIteratorType
  }

  this.log({iteratorType: type, sequenceNumber: sequenceNumber}, 'Updating shard iterator')
  kinesis.getShardIterator(this.client, this.streamName, this.shardId, type, sequenceNumber, function (err, data) {
    if (err) return callback(err)

    _this.log(data, 'Updated shard iterator')
    _this.nextShardIterator = data.ShardIterator
    callback()
  })
}

/**
 * Exit the consumer.
 *
 * @param  {Error=} err
 */
AbstractConsumer.prototype._exit = function (err) {
  var _this = this

  if (this.hasStartedExit) return
  this.hasStartedExit = true

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


/**
 * Create a child consumer.
 *
 * @static
 * @param  {{processRecords: Function, initialize: Function=, shutdown: Function=}} args
 * @return {AbstractConsumer}
 */
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
