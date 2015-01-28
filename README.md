# Node Kinesis Client Library

[![Build Status](https://travis-ci.org/evansolomon/nodejs-kinesis-client-library.svg?branch=master)](https://travis-ci.org/evansolomon/nodejs-kinesis-client-library)

Based on the AWS [Kinesis Client Library for Java](https://github.com/awslabs/amazon-kinesis-client), reimplemented in Node.js.

* Build Kinesis consumers easily
* Automatically scale up or down as streams split or merge
* Allow distributed processing without any extra code

Install with `npm install kinesis-client-library --save`.

I am using this in production, but it's very new so there's a good chance it will (1) have some bugs and (2) go through a breaking change at some point. Feedback welcome, use with care.

## Why?

I had a stack in Node.js. Working with and deploying a Java app just to consume a stream was too inconvenient, so I made this.


## Terminology

* **Consumer**: A single process that is responsible for consuming a single Kinesis shard. Consumers are created and managed by clusters; they should not be created directly directly.
* **Cluster**: A process that manages consumers. A single cluster can have many consumers (or 1, or 0) that all run on the same machine.  Clusters should be created by the [CLI](#cli).
* **Network**: All of the consumers processing a single Kinesis stream for a single application. Networks can have many clusters that are run on one or many machines.


Networks are meant to be distributed (though they don't have to be) and will automatically rebalance shards across clusters. Each cluster will try to process an equal number of shards, so it's a good idea to make sure each cluster has equivalent resources (e.g. clusters should have similar memory and CPU available). Networks will automatically pick up new shards as [splits](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_SplitShard.html) or [merges](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_MergeShards.html) happen.

Multiple networks can independently process the same stream as long as the stream has enough capacity. In this case, you should be sure to give each set of processors a unique `table` flag.


## Usage

Before you can do anything with this library, you need to create a Kinesis stream. Everything from this point forward assumes you have an existing stream.


### CLI

Clusters are designed to be launched from the command line.  The library exposes a `launch-kinesis-cluster` executable.

```
$ launch-kinesis-cluster --help
Usage:

--help  (Display this message)

Required flags:
--consumer [Path to consumer file]
--table [DynamoDB table name]
--stream [Kinesis stream name]

Optional flags:
--start-at [Starting iterator type] ("trim_horizon" or "latest", defaults to "trim_horizon")
--capacity.[read|write] [Throughput] (DynamoDB throughput for *new* tables, defaults to 10 for each)
--aws.[option] [Option value]  (e.g. --aws.region us-west-2)
--http [port]  (Start HTTP server, port default to $PORT)
--local-dynamo (Whether or not to use a local implementation of DynamoDB, defaults to false)
--local-dynamo-directory (Directory to store local DB, defaults to temp directory)
```

Notes:
* `table` is used as a DynamoDB table name that is unique to the network. If the table doesn't exist yet, it is created.
* `stream` must match a Kinesis stream that *already exists*.
* The (optional) HTTP server is primarily meant to be used as a health check.
* AWS options are based to the [`AWS.Service` constructor](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Service.html), so any other credential strategies (e.g. environment variables, EC2 IAM roles) will be used automatically.


### Consumer

Consumers are implemented in JavaScript by calling `AbstractConsumer.extend()` with an object that implements some/all of these methods:

* `processRecords` (required): Accepts an array of [Record](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html) objects (`Record.Data` will be a `Buffer` object) and a callback. Pass `true` as the second argument to the callback when you want to save a checkpoint — i.e. when you have processed a chunk of data.
* `initialize` (optional): Called when a consumer is spawned, before any records are processed. Accepts a callback that must be called to start record processing.
* `shutdown` (optional): Called when a consumer is about to exit. Accepts a callback that must be called to complete shutdown; if the callback is not called without 30 seconds the process exits anyway.

#### Example consumer implementation

This consumer uploads records to S3 in 50 megabyte batches.

```js
// AWS config skipped for brevity
var s3 = new require('aws-sdk').S3()
var kcl = require('kinesis-client-library')

var newlineBuffer = new Buffer('\n')

kcl.AbstractConsumer.extend({
  // create places to hold some data about the consumer
  initialize: function (done) {
    this.cachedRecords = []
    this.cachedRecordSize = 0
    // This MUST be called or processing will never start
    // That is really really really bad
    done()
  },

  processRecords: function (records, done) {
    // Put each record into our list of cached records (separated by newlines) and update the size
    records.forEach(function (record) {
      this.cachedRecords.push(record.Data)
      this.cachedRecords.push(newlineBuffer)
      this.cachedRecordSize += (record.Data.length + newlineBuffer.length)
    }.bind(this))

    // not very good for performance
    var shouldCheckpoint = this.cachedRecordSize > 50000000

    // Get more records, but not save a checkpoint
    if (! shouldCheckpoint) return done()

    // Upload the records to S3
    s3.putObject({
      Bucket: 'my-bucket-name',
      Key: 'path/to/records/' + Date.now(),
      Body: Buffer.concat(this.cachedRecords)
    }, function (err)  {
      if (err) return done(err)

      this.cachedRecords = []
      this.cachedRecordsSize = 0

      // Pass `true` to checkpoint the latest record we've received
      done(null, true)
    }.bind(this))
  }
})
```

#### Example consumer launch

Assume the code above is in `lib/consumer.js` and you've got a package with `kinesis-client-library` declared as a dependency.

For convenience, we'll assume your AWS credentials are defined as [environment variables](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/EnvironmentCredentials.html). Using the command line, you could launch a cluster like this.

```
launch-kinesis-cluster \
  --consumer lib/consumer.js \
  --table MyKinesisConsumerApp \
  --stream MyKinesisStreamName \
  --aws.region us-east-1
```
