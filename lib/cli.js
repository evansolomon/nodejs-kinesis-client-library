/**
 * @fileoverview Create a cluster from the command line.
 */

var path = require('path')
var minimist = require('minimist')
var logger = require('bunyan').createLogger({name: 'KinesisClusterCLI'})

var ConsumerCluster = require('../ConsumerCluster')

var args = minimist(process.argv.slice(2))

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
  process.exit()
}

var consumer = path.resolve(process.env.PWD, args.consumer || '')
var opts = {
  tableName: args.table,
  streamName: args.stream,
  awsConfig: args.aws,
  startingIteratorType: args['start-at'],
  capacity: args.capacity
}

logger.info('Consumer app path:', consumer)
var clusterOpts = Object.keys(opts).reduce(function (memo, key) {
  if (opts[key] !== undefined) {
    memo[key] = opts[key]
  }

  return memo
}, {})
logger.info({options: clusterOpts}, 'Cluster options')

logger.info('Launching cluster')
var cluster
try {
  cluster = new ConsumerCluster(consumer, opts)
  cluster.init()
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
