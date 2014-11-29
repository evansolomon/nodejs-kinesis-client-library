var path = require('path')
var bunyan = require('bunyan')
var minimist = require('minimist')
var ConsumerCluster = require('../ConsumerCluster')

var logger = bunyan.createLogger({name: 'KinesisClusterCLI'})

module.exports.run = function (argv) {
  var args = minimist(argv)

  var consumer = path.resolve(process.env.PWD, args.consumer || '')
  var opts = {
    tableName: args.table,
    streamName: args.stream,
    awsConfig: args.aws
  }

  logger.info('Consumer app path:', consumer)
  logger.info('Cluster options:')
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
  } catch (e) {
    logger.error('Error launching cluster')
    logger.error(e)
    process.exit(1)
  }

  logger.info('Spawned cluster %s', cluster.id)
  cluster.init()

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
}
