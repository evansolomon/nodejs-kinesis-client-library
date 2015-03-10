var child_process = require('child_process')
var path = require('path')

var kinesalite = require('./kinesalite')

exports.launch = function (name) {
  var launch = path.resolve(__dirname, '../../bin/launch')
  var consumer = path.join(__dirname, 'consumers', name + '.js')
  return child_process.spawn(launch, [
    '--consumer', consumer,
    '--stream', kinesalite.name,
    '--table', 'PrintConsumerTable',
    '--local-dynamo',
    '--local-kinesis',
    '--local-kinesis-port', kinesalite.port,
    '--local-kinesis-no-start',
    '--aws.region', 'us-east-1',
    '--aws.accessKeyId', 'accessKeyId',
    '--aws.secretAccessKey', 'secretAccessKey'
  ])
}
