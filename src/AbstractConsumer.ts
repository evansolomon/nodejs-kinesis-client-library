import util = require('util')

import _ = require('underscore')
import AWS = require('aws-sdk')
import async = require('async')
import bunyan = require('bunyan')

import awsFactory = require('./lib/aws/factory')
import config = require('./lib/config')
import kinesis = require('./lib/aws/kinesis')

import lease = require('./lib/models/Lease')


interface AbstractConsumerOpts {
  streamName: string
  shardId: string
  leaseCounter?: number
  tableName: string
  awsConfig: AWS.ClientConfig
  startingIteratorType?: string
  localDynamo: Boolean
}

export interface ProcessRecordsCallback {
  (err: any, checkpointSequenceNumber?: Boolean|string): void;
}

export interface ConsumerExtension {
  processRecords: (records: AWS.kinesis.Record[], callback: ProcessRecordsCallback) => void
  initialize?: (callback: (err?: any) => void) => void
  shutdown?: (callback: (err?: any) => void) => void
}

// Stream consumer, meant to be extended.
export class AbstractConsumer {
  public static DEFAULT_SHARD_ITERATOR_TYPE = 'TRIM_HORIZON'
  public static ShardIteratorTypes = {
    AT_SEQUENCE_NUMBER: 'AT_SEQUENCE_NUMBER',
    AFTER_SEQUENCE_NUMBER: 'AFTER_SEQUENCE_NUMBER',
    TRIM_HORIZON: 'TRIM_HORIZON',
    LATEST: 'LATEST'
  }
  public logger: bunyan.Logger
  private opts: AbstractConsumerOpts
  private lease: lease.Model
  private maxSequenceNumber: string
  private kinesis: AWS.Kinesis
  private nextShardIterator: string
  private hasStartedExit = false

  // Called before record processing starts. This method may be implemented by the child.
  // If it is implemented, the callback must be called for processing to begin.
  public initialize (callback: (err?: Error) => void) {
    this.log('No initialize method defined, skipping')
    callback()
  }

  // Process a batch of records. This method must be implemented by the child.
  public processRecords (records: AWS.kinesis.Record[], callback: ProcessRecordsCallback) {
    throw new Error('processRecords must be defined by the consumer class')
  }

  // Called before a consumer exits. This method may be implemented by the child.
  public shutdown (callback: (err?: Error) => void) {
    this.log('No shutdown method defined, skipping')
    callback()
  }

  constructor(opts) {
    this.opts = opts

    if (! this.opts.startingIteratorType) {
      this.opts.startingIteratorType = AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE
    }

    this.kinesis = awsFactory.kinesis(this.opts.awsConfig, false)

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

    this._init()

    if (! this.opts.shardId) {
      this._exit(new Error('Cannot spawn a consumer without a shard ID'))
    }
  }

  private _init () {
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

  public log (...args: any[]) {
    this.logger.info.apply(this.logger, args)
  }

  // Continuously fetch records from the stream.
  private _loopGetRecords () {
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

  // Continuously update this consumer's lease reservation.
  private _loopReserveLease () {
    var _this = this

    this.log('Starting reserveLease loop')

    async.forever(function (done) {
      setTimeout(_this._reserveLease.bind(_this, done), 5000)
    }, function (err) {
      _this._exit(err)
    })
  }

  // Setup the initial lease reservation state.
  private _setupLease () {
    var id = this.opts.shardId
    var leaseCounter = this.opts.leaseCounter || null
    var tableName = this.opts.tableName
    var awsConfig = this.opts.awsConfig
    var localDynamo = !! this.opts.localDynamo

    this.log({leaseCounter: leaseCounter, tableName: tableName}, 'Setting up lease')

    this.lease = new lease.Model(id, leaseCounter, tableName, awsConfig, localDynamo)
  }

  // Update the lease in the network database.
  private _reserveLease (callback) {
    this.logger.debug('Reserving lease')
    this.lease.reserve(callback)
  }

  // Mark the consumer's shard as finished, then exit.
  private _markFinished () {
    var _this = this
    this.log('Marking shard as finished')

    this.lease.markFinished(function (err) {
      _this._exit(err)
    })
  }

  // Get records from the stream and wait for them to be processed.
  private _getRecords (callback) {
    var _this = this

    this.kinesis.getRecords({ShardIterator: this.nextShardIterator}, function (err, data) {
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
      if (data.NextShardIterator == null && (! data.Records || data.Records.length === 0)) {
        return _this._markFinished()
      }

      var lastSequenceNumber = _.pluck(data.Records, 'SequenceNumber').pop()
      _this.maxSequenceNumber = lastSequenceNumber || _this.maxSequenceNumber

      _this._processRecords(data.Records, callback)
    })
  }

  // Wrap the child's processRecords method to handle checkpointing.
  private _processRecords (records, callback) {
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

      _this.lease.checkpoint(<string> checkpointSequenceNumber, callback)
    })
  }

  // Get a new shard iterator from Kinesis.
  private _updateShardIterator (sequenceNumber, callback) {
    var _this = this
    var type
    if (sequenceNumber) {
      type = AbstractConsumer.ShardIteratorTypes.AFTER_SEQUENCE_NUMBER
    } else {
      type = this.opts.startingIteratorType
    }

    this.log({iteratorType: type, sequenceNumber: sequenceNumber}, 'Updating shard iterator')

    var params = {
      StreamName: this.opts.streamName,
      ShardId: this.opts.shardId,
      ShardIteratorType: type,
      StartingSequenceNumber: sequenceNumber
    }

    this.kinesis.getShardIterator(params, function (e, data) {
      if (e) return callback(e)

      _this.log(data, 'Updated shard iterator')
      _this.nextShardIterator = data.ShardIterator
      callback()
    })
  }

  // Exit the consumer with its optional shutdown process.
  private _exit (err) {
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

  // Create a child consumer.
  public static extend (args: ConsumerExtension) {
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

    new Ctor()
  }
}
