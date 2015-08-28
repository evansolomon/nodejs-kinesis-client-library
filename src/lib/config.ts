const config = {
  shutdownMessage: 'shutdown',
  localDynamoDBEndpoint: {
    protocol: 'http',
    hostname: 'localhost',
    port: '6789'
  },
  localKinesisEndpoint: {
    protocol: 'http',
    hostname: 'localhost',
    port: '4567'
  }
}

export default config
