import {series, forever} from 'async'
import {ClientConfig, Kinesis, kinesis} from 'aws-sdk'
import {Logger, createLogger} from 'bunyan'
import {pluck} from 'underscore'

import {createKinesisClient} from './lib/aws/factory'
import config from './lib/config'
import {Lease} from './lib/models/Lease'


interface AbstractConsumerOpts {
  streamName: string
  shardId: string
  leaseCounter?: number
  tableName: string
  awsConfig: ClientConfig
  startingIteratorType?: string
  dynamoEndpoint?: string
  kinesisEndpoint?: string
  logLevel?: string
  numRecords?: number
  timeBetweenReads?: number
}

export interface ProcessRecordsCallback {
  (err: any, checkpointSequenceNumber?: Boolean | string): void;
}

export interface ConsumerExtension {
  processResponse?: (request: kinesis.GetRecordsResult, callback: ProcessRecordsCallback) => void
  processRecords?: (records: kinesis.Record[], callback: ProcessRecordsCallback) => void
  initialize?: (callback: (err?: any) => void) => void
  shutdown?: (callback: (err?: any) => void) => void
}

// Stream consumer, meant to be extended.
export class AbstractConsumer {
  public static ABSTRACT_METHODS = ['processRecords', 'initialize', 'shutdown']
  public static DEFAULT_SHARD_ITERATOR_TYPE = 'TRIM_HORIZON'
  public static DEFAULT_TIME_BETWEEN_READS = 1000
  public static ShardIteratorTypes = {
    AT_SEQUENCE_NUMBER: 'AT_SEQUENCE_NUMBER',
    AFTER_SEQUENCE_NUMBER: 'AFTER_SEQUENCE_NUMBER',
    TRIM_HORIZON: 'TRIM_HORIZON',
    LATEST: 'LATEST',
  }
  public logger: Logger
  private opts: AbstractConsumerOpts
  private lease: Lease
  private maxSequenceNumber: string
  private kinesis: Kinesis
  private nextShardIterator: string
  private hasStartedExit = false
  private timeBetweenReads: number
  private throughputErrorDelay: number

  // Called before record processing starts. This method may be implemented by the child.
  // If it is implemented, the callback must be called for processing to begin.
  public initialize(callback: (err?: Error) => void) {
    this.log('No initialize method defined, skipping')
    callback()
  }

  // Process a batch of records. This method, or processResponse, must be implemented by the child.
  public processRecords(records: kinesis.Record[], callback: ProcessRecordsCallback) {
    throw new Error('processRecords must be defined by the consumer class')
  }

  // Process raw kinesis response.  Override it to get access to the MillisBehindLatest field.
  public processResponse(response: kinesis.GetRecordsResult, callback: ProcessRecordsCallback) {
    this.processRecords(response.Records, callback)
  }

  // Called before a consumer exits. This method may be implemented by the child.
  public shutdown(callback: (err?: Error) => void) {
    this.log('No shutdown method defined, skipping')
    callback()
  }

  constructor(opts) {
    this.opts = opts

    this.timeBetweenReads = opts.timeBetweenReads || AbstractConsumer.DEFAULT_TIME_BETWEEN_READS
    this.resetThroughputErrorDelay()

    if (!this.opts.startingIteratorType) {
      this.opts.startingIteratorType = AbstractConsumer.DEFAULT_SHARD_ITERATOR_TYPE
    }

    this.kinesis = createKinesisClient(this.opts.awsConfig, this.opts.kinesisEndpoint)

    process.on('message', msg => {
      if (msg === config.shutdownMessage) {
        this.exit(null)
      }
    })

    this.logger = createLogger({
      name: 'KinesisConsumer',
      level: opts.logLevel,
      streamName: opts.streamName,
      shardId: opts.shardId,
    })

    this.init()

    if (!this.opts.shardId) {
      this.exit(new Error('Cannot spawn a consumer without a shard ID'))
    }
  }

  private init() {
    this.setupLease()

    series([
      this.initialize.bind(this),
      this.reserveLease.bind(this),
      done => {
        this.lease.getCheckpoint((err, checkpoint) => {
          if (err) {
            return done(err)
          }

          this.log({ checkpoint: checkpoint }, 'Got starting checkpoint')
          this.maxSequenceNumber = checkpoint
          this.updateShardIterator(checkpoint, done)
        })
      },
    ], err => {
      if (err) {
        return this.exit(err)
      }

      this.loopGetRecords()
      this.loopReserveLease()
    })
  }

  public log(...args: any[]) {
    this.logger.info.apply(this.logger, args)
  }

  // Continuously fetch records from the stream.
  private loopGetRecords() {
    const timeBetweenReads = this.timeBetweenReads

    this.log('Starting getRecords loop')

    forever(done => {
      const gotRecordsAt = Date.now()

      this.getRecords(err => {
        if (err) {
          return done(err)
        }

        const timeToWait = Math.max(0, timeBetweenReads - (Date.now() - gotRecordsAt))

        if (timeToWait > 0) {
          setTimeout(done, timeToWait)
        } else {
          done()
        }
      })
    }, err => {
      this.exit(err)
    })
  }

  // Continuously update this consumer's lease reservation.
  private loopReserveLease() {
    this.log('Starting reserveLease loop')

    forever(done => {
      setTimeout(this.reserveLease.bind(this, done), 5000)
    }, err => {
      this.exit(err)
    })
  }

  // Setup the initial lease reservation state.
  private setupLease() {
    const id = this.opts.shardId
    const leaseCounter = this.opts.leaseCounter || null
    const tableName = this.opts.tableName
    const awsConfig = this.opts.awsConfig

    this.log({ leaseCounter: leaseCounter, tableName: tableName }, 'Setting up lease')

    this.lease = new Lease(id, leaseCounter, tableName, awsConfig, this.opts.dynamoEndpoint)
  }

  // Update the lease in the network database.
  private reserveLease(callback) {
    this.logger.debug('Reserving lease')
    this.lease.reserve(callback)
  }

  // Mark the consumer's shard as finished, then exit.
  private markFinished() {
    this.log('Marking shard as finished')

    this.lease.markFinished(err => this.exit(err))
  }

  // Get records from the stream and wait for them to be processed.
  private getRecords(callback) {
    let getRecordsParams = <kinesis.GetRecordsRequest>{ ShardIterator: this.nextShardIterator }
    if (this.opts.numRecords && this.opts.numRecords > 0) {
      getRecordsParams = { ShardIterator: this.nextShardIterator, Limit: this.opts.numRecords }
    }

    this.kinesis.getRecords(getRecordsParams, (err, data) => {
      // Handle known errors
      if (err && err.code === 'ExpiredIteratorException') {
        this.log('Shard iterator expired, updating before next getRecords call')
        return this.updateShardIterator(this.maxSequenceNumber, err => {
          if (err) {
            return callback(err)
          }

          this.getRecords(callback)
        })
      }

      if (err && err.code === 'ProvisionedThroughputExceededException') {
        this.log('Provisioned throughput exceeded, pausing before next getRecords call', {
          delay: this.throughputErrorDelay,
        })
        return setTimeout(() => {
          this.increaseThroughputErrorDelay()
          this.getRecords(callback)
        }, this.throughputErrorDelay)
      }

      this.resetThroughputErrorDelay()

      // We have an error but don't know how to handle it
      if (err) {
        return callback(err)
      }

      // Save this in case we need to checkpoint it in a future request before we get more records
      if (data.NextShardIterator != null) {
        this.nextShardIterator = data.NextShardIterator
      }

      // We have processed all the data from a closed stream
      if (data.NextShardIterator == null && (!data.Records || data.Records.length === 0)) {
        this.log({ data: data }, 'Marking shard as finished')
        return this.markFinished()
      }

      const lastSequenceNumber = pluck(data.Records, 'SequenceNumber').pop()
      this.maxSequenceNumber = lastSequenceNumber || this.maxSequenceNumber

      this.wrappedProcessResponse(data, callback)
    })
  }

  // Wrap the child's processResponse method to handle checkpointing.
  private wrappedProcessResponse(data, callback) {
    this.processResponse(data, (err, checkpointSequenceNumber) => {
      if (err) {
        return callback(err)
      }

      // Don't checkpoint
      if (!checkpointSequenceNumber) {
        return callback()
      }
      // We haven't actually gotten any records so there is nothing to checkpoint
      if (!this.maxSequenceNumber) {
        return callback()
      }

      // Default case to checkpoint the latest sequence number
      if (checkpointSequenceNumber === true) {
        checkpointSequenceNumber = this.maxSequenceNumber
      }

      this.lease.checkpoint(<string>checkpointSequenceNumber, callback)
    })
  }

  // Get a new shard iterator from Kinesis.
  private updateShardIterator(sequenceNumber, callback) {
    let type
    if (sequenceNumber) {
      type = AbstractConsumer.ShardIteratorTypes.AFTER_SEQUENCE_NUMBER
    } else {
      type = this.opts.startingIteratorType
    }

    this.log({ iteratorType: type, sequenceNumber: sequenceNumber }, 'Updating shard iterator')

    const params = {
      StreamName: this.opts.streamName,
      ShardId: this.opts.shardId,
      ShardIteratorType: type,
      StartingSequenceNumber: sequenceNumber,
    }

    this.kinesis.getShardIterator(params, (e, data) => {
      if (e) {
        return callback(e)
      }

      this.log(data, 'Updated shard iterator')
      this.nextShardIterator = data.ShardIterator
      callback()
    })
  }

  // Exit the consumer with its optional shutdown process.
  private exit(err) {
    if (this.hasStartedExit) {
      return
    }

    this.hasStartedExit = true

    if (err) {
      this.logger.error(err)
    }

    setTimeout(() => {
      this.logger.error('Forcing exit based on shutdown timeout')
      // Exiting with 1 because the shutdown process took too long
      process.exit(1)
    }, 30000)

    this.log('Starting shutdown')
    this.shutdown(() => {
      const exitCode = err == null ? 0 : 1
      process.exit(exitCode)
    })
  }

  private increaseThroughputErrorDelay() {
    this.throughputErrorDelay = this.throughputErrorDelay * 2
  }

  private resetThroughputErrorDelay() {
    this.throughputErrorDelay = this.timeBetweenReads
  }

  // Create a child consumer.
  public static extend(args: ConsumerExtension) {
    const opts = JSON.parse(process.env.CONSUMER_INSTANCE_OPTS)
    class Consumer extends AbstractConsumer {
      constructor() {
        super(opts)

        AbstractConsumer.ABSTRACT_METHODS
          .filter(method => args[method])
          .forEach(method => this[method] = args[method])
      }
    }

    new Consumer()
  }
}
