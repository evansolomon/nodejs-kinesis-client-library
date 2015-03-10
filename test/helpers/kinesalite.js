var child_process = require('child_process')
var path = require('path')

var AWS = require('aws-sdk')
var async = require('async')

var launchString = 'Listening'

exports.region = 'us-east-1'
exports.name = 'test-stream'
exports.port = '5678'
exports.shardCount = 3

exports.start = function (callback) {
  var kinesalite = path.resolve(__dirname, '../../node_modules/.bin/kinesalite')
  var proc = child_process.spawn(kinesalite, ['--port', exports.port])
  proc.on('error', callback)

  var launched = false
  var stdout = ''
  proc.stdout.on('data', function (chunk) {
    if (launched) return

    stdout += chunk
    if (stdout.indexOf(launchString) === -1) return

    launched = true
    callback(null, proc)
  })
}

exports.createStream = function (callback) {
  var kinesis = new AWS.Kinesis({region: exports.region})
  kinesis.setEndpoint('http://localhost:' + exports.port)
  kinesis.createStream({
    StreamName: exports.name,
    ShardCount: exports.shardCount
  }, function (e) {
    if (e) return callback(e)

    var isActive = false
    async.doUntil(function (cb) {
      kinesis.describeStream({StreamName: exports.name}, function (err, desc) {
        if (err) return cb(err)
        isActive = desc.StreamDescription.StreamStatus === 'ACTIVE'
        cb()
      })
    }, function () {
      return isActive
    }, callback)
  })
}
