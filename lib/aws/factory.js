var AWS = require('aws-sdk')

module.exports = function (config, service) {
  var Ctor = AWS[service]
  return new Ctor(config)
}
