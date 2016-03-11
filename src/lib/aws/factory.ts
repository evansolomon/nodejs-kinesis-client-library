import {ClientConfig, DynamoDB, Kinesis} from 'aws-sdk'

export const createKinesisClient = (conf: ClientConfig, endpoint?: string): Kinesis => {
  const instance = new Kinesis(conf || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}

export const createDynamoClient = (conf: ClientConfig, endpoint?: string): DynamoDB => {
  const instance = new DynamoDB(conf || {})
  if (endpoint) {
    instance.setEndpoint(endpoint)
  }

  return instance
}
