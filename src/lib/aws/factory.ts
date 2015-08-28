import * as AWS from 'aws-sdk'

const kinesis = (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.Kinesis => {
  const instance = new AWS.Kinesis(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

const dynamo = (awsConfig:AWS.ClientConfig, endpoint?:string) : AWS.DynamoDB => {
  const instance = new AWS.DynamoDB(awsConfig || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

export {kinesis, dynamo}
