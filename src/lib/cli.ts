import os = require('os')
import path = require('path')
import child_process = require('child_process')

import AWS = require('aws-sdk')
import async = require('async')
import minimist = require('minimist')
import mkdirp = require('mkdirp')
import bunyan = require('bunyan')

import ConsumerCluster = require('../ConsumerCluster')
import config = require('./config')

var logger = bunyan.createLogger({name: 'KinesisClusterCLI'})

interface KinesisCliArgs extends minimist.ParsedArgs {
  help: Boolean
  consumer: string
  table: string
  stream: string
  'start-at'?: string
  capacity?: {
    read?: number
    write?: number
  }
  aws?: AWS.ClientConfig
  http?: (Boolean|number)
  'local-dynamo'?: Boolean
  'local-dynamo-directory'?: string
}

var args = <KinesisCliArgs> minimist(process.argv.slice(2))

if (args.help) {
  console.log('Usage:\n')
  console.log('--help  (Display this message)')

  console.log()
  console.log('Required flags:')
  console.log('--consumer [Path to consumer file]')
  console.log('--table [DynamoDB table name]')
  console.log('--stream [Kinesis stream name]')

  console.log()
  console.log('Optional flags:')
  console.log('--start-at [Starting iterator type] ("trim_horizon" or "latest", defaults to "trim_horizon")')
  console.log('--capacity.[read|write] [Throughput] (DynamoDB throughput for *new* tables, defaults to 10 for each)')
  console.log('--aws.[option] [Option value]  (e.g. --aws.region us-west-2)')
  console.log('--http [port]  (Start HTTP server, port defaults to $PORT)')
  console.log('--local-dynamo (Whether or not to use a local implementation of DynamoDB, defaults to false)')
  console.log('--local-dynamo-directory (Directory to store local DB, defaults to temp directory)')
  process.exit()
}

var consumer = path.resolve(process.env.PWD, args.consumer || '')
var opts = {
  tableName: args.table,
  streamName: args.stream,
  awsConfig: args.aws,
  startingIteratorType: args['start-at'],
  capacity: args.capacity,
  localDynamo: !! args['local-dynamo']
}

logger.info('Consumer app path:', consumer)
var clusterOpts = Object.keys(opts).reduce(function (memo, key) {
  if (opts[key] !== undefined) {
    memo[key] = opts[key]
  }

  return memo
}, {})
logger.info({options: clusterOpts}, 'Cluster options')

async.auto({
  localDynamo: function (done) {
    if (! opts.localDynamo) return done()
    logger.info('Launching local DynamoDB')

    var databaseDir = args['local-dynamo-directory']
    if (! databaseDir) {
      databaseDir = path.join(os.tmpdir(), 'localdynamo', Date.now().toString())
    }

    logger.info({directory: databaseDir}, 'Creating directory for Local DynamoDB')
    try {
      mkdirp.sync(databaseDir)
    } catch (e) {
      return done(e)
    }

    // If you ran this with some unusual node executable, let's keep the good times going
    var nodeExecutable = process.argv[0]
    var proc = child_process.spawn(nodeExecutable, [
      './node_modules/local-dynamo/bin/launch_local_dynamo.js',
      '--database_dir', databaseDir,
      '--port', config.localDynamoDBEndpoint.port.toString()
    ], {
      cwd: path.resolve(__dirname, '../..')
    })

    proc.stdout.pipe(process.stdout)
    proc.stderr.pipe(process.stderr)

    proc.on('exit', function (code) {
      process.exit(code)
    }).on('error', function (err) {
      logger.error(err, 'Error in local DynamoDB')
      process.exit(1)
    })

    // Local Dynamo writes some stuff to stderr when it's ready
    var finishedStart = false
    proc.stderr.on('data', function () {
      if (finishedStart) return

      finishedStart = true
      // little delay just in case
      setTimeout(done, 500)
    })
  },
  cluster: ['localDynamo', function (done) {
    logger.info('Launching cluster')
    var cluster
    try {
      cluster = new ConsumerCluster.ConsumerCluster(consumer, opts)
    } catch (e) {
      logger.error('Error launching cluster')
      logger.error(e)
      process.exit(1)
    }

    logger.info('Spawned cluster %s', cluster.cluster.id)

    if (args.http) {
      var port
      if (typeof args.http === 'number') {
        port = args.http
      } else {
        port = process.env.PORT
      }

      logger.info('Spawning HTTP server on port %d', port)
      cluster.serveHttp(port)
    }
  }]
}, function (err) {
  if (err) {
    logger.error(err)
    process.exit(1)
  }
})
