/**
 * @fileoverview Wrap construction for AWS services.
 */

import AWS = require('aws-sdk')

export var kinesis = function (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.Kinesis {
  var instance =  new AWS.Kinesis(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

export var dynamo = function (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.DynamoDB {
  var instance =  new AWS.DynamoDB(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}
