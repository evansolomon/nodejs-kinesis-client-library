/**
 * @fileoverview Wrap construction for AWS services.
 */

var url = require('url')
var AWS = require('aws-sdk')

var config = require('../config')

/**
 * Create an AWS service instance.
 *
 * @param  {?Object}      awsConfig  Configuration for the service.
 * @param  {Boolean}      local      Whether or not to use a local implementation of this service.
 * @param  {string}       service    Service constructor name.
 * @return {AWS.Service}
 */
module.exports.create = function (awsConfig, local, service) {
  var Ctor = AWS[service]
  var instance =  new Ctor(awsConfig || {})

  if (local) {
    var endpointConfigName = 'local' + service + 'Endpoint'
    var endpointConfig = config[endpointConfigName]
    var endpoint = url.format(endpointConfig)
    instance.setEndpoint(endpoint)
  }

  return instance
}
