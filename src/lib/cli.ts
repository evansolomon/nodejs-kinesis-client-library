import * as os from 'os'
import * as path from 'path'
import * as child_process from 'child_process'

import * as async from 'async'
import * as AWS from 'aws-sdk'
import * as bunyan from 'bunyan'
import * as minimist from 'minimist'
import * as mkdirp from 'mkdirp'

import config from './config'
import * as ConsumerCluster from '../ConsumerCluster'

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
  'dynamo-endpoint'?: string
  'local-dynamo'?: Boolean
  'local-dynamo-directory'?: string
  'kinesis-endpoint'?: string
  'local-kinesis'?: Boolean
  'local-kinesis-port'?: string
  'local-kinesis-no-start'?: Boolean
  'log-level': string
  'num-records'?: number
  'time-between-reads'?: number
}

var args = <KinesisCliArgs> minimist(process.argv.slice(2))
var logger = bunyan.createLogger({
  name: 'KinesisClusterCLI',
  level: args['log-level']
})

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
  console.log('--log-level [level] (Logging verbosity, uses Bunyan log levels)')
  console.log('--dynamo-endpoint (Use a cusotm endpoint for the DynamoDB service)')
  console.log('--local-dynamo (Whether or not to use a local implementation of DynamoDB, defaults to false)')
  console.log('--local-dynamo-directory (Directory to store local DB, defaults to temp directory)')
  console.log('--kinesis-endpoint (Use a custom endpoint for the Kinesis service)')
  console.log('--local-kinesis (Use a local implementation of Kinesis, defaults to false)')
  console.log('--local-kinesis-port (Port to access local Kinesis on, defaults to 4567)')
  console.log('--local-kinesis-no-start (Assume a local Kinesis server is already running, defaults to false)')
  console.log('--num-records (Maximum number of records to get in each Kinesis query, defaults to the Kinesis maximum of 10000)')
  console.log('--time-between-reads (Time to wait between fetching records in milliseconds, defaults to 1000)')
  process.exit()
}

var consumer = path.resolve(process.env.PWD, args.consumer || '')
var opts = {
  tableName: args.table,
  streamName: args.stream,
  awsConfig: args.aws,
  startingIteratorType: args['start-at'],
  capacity: args.capacity,
  dynamoEndpoint: args['dynamo-endpoint'],
  localDynamo: !! args['local-dynamo'],
  kinesisEndpoint: args['kinesis-endpoint'],
  localKinesis: !! args['local-kinesis'],
  localKinesisPort: args['local-kinesis-port'],
  logLevel: args['log-level'],
  numRecords: args['num-records'],
  timeBetweenReads: args['time-between-reads'],
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
    if (! opts.localDynamo) {
      return done()
    }

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
      if (finishedStart) {
        return
      }

      finishedStart = true
      // little delay just in case
      setTimeout(done, 500)
    })
  },
  localKinesis: function (done) {
    if (! opts.localKinesis) {
      return done()
    }
    if (args['local-kinesis-no-start']) {
      return done()
    }

    var port = args['local-kinesis-port'] || config.localKinesisEndpoint.port

    var proc = child_process.spawn('./node_modules/.bin/kinesalite', [
      '--port', port.toString()
    ], {
      cwd: path.resolve(__dirname, '../..')
    })

    proc.on('error', function (err) {
      logger.error(err, 'Error in local Kinesis')
      process.exit(1)
    })

    var timer = setTimeout(function () {
      done(new Error('Local Kinesis took too long to start'))
    }, 5000)

    var output = ''
    proc.stdout.on('data', function (chunk) {
      output += chunk
      if (output.indexOf('Listening') === -1) {
        return
      }

      done()
      done = function () {}
      clearTimeout(timer)
    })
  },
  cluster: ['localDynamo', 'localKinesis', function (done) {
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
