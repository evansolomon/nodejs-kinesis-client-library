/**
 * @fileoverview Wrap construction for AWS services.
 */

var AWS = require('aws-sdk')

/**
 * Create an AWS service instance.
 *
 * @param  {?Object}      config   Configuration for the service.
 * @param  {string}       service  Service constructor name.
 * @return {AWS.Service}
 */
module.exports.create = function (config, service) {
  var Ctor = AWS[service]
  return new Ctor(config || {})
}
