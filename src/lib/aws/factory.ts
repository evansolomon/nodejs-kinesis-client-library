import * as AWS from 'aws-sdk'

let kinesis = function (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.Kinesis {
  var instance =  new AWS.Kinesis(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

let dynamo = function (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.DynamoDB {
  var instance =  new AWS.DynamoDB(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

export {kinesis, dynamo}
