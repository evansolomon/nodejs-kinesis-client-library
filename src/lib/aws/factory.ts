/**
 * @fileoverview Wrap construction for AWS services.
 */

import url = require('url')
import AWS = require('aws-sdk')

import config = require('../config')

function localizeService(service: AWS.Service, configName: string) {
  var endpointConfig = config[configName]
  var endpoint = url.format(endpointConfig)
  service.setEndpoint(endpoint)
}

export var kinesis = function (awsConfig:AWS.ClientConfig, local:Boolean, port?:number) : AWS.Kinesis {
  var instance =  new AWS.Kinesis(awsConfig || {})
  if (local) {
    localizeService(instance, 'localKinesisEndpoint')
  }

  // Allow port to be customized so that we can share an existing stream with other processes
  if (port) {
    instance.endpoint.port = port
  }

  return instance
}

export var dynamo = function (awsConfig:AWS.ClientConfig, local:Boolean) : AWS.DynamoDB {
  var instance =  new AWS.DynamoDB(awsConfig || {})
  if (local) {
    localizeService(instance, 'localDynamoDBEndpoint')
  }

  return instance
}
