import * as AWS from 'aws-sdk'
import * as async from 'async'

export class Stream {
  private name: string
  private kinesis: AWS.Kinesis

  constructor(name: string, kinesis: AWS.Kinesis) {
    this.name = name
    this.kinesis = kinesis
  }

  public exists (callback) {
    this.describe(function (err) {
      if (err && err.code === 'ResourceNotFoundException') {
        return callback(null, false)
      }
      if (err) {
        return callback(err)
      }
      callback(null, true)
    })
  }

  public onActive (callback) {
    let state = {isActive: false, isDeleting: false}
    async.auto({
      isActive: done => {
        this.isActive((err, isActive) => {
          state.isActive = isActive
          done(err)
        })
      },

      isDeleting: ['isActive', done => {
        if (state.isActive) {
          return done()
        }

        this.isDeleting(function (err, isDeleting) {
          state.isDeleting = isDeleting
          done(err)
        })
      }]
    }, err => {
      if (err) {
        return callback(err)
      }

      if (state.isActive) {
        return callback()
      }

      if (state.isDeleting) {
        return callback(new Error('Stream is deleting'))
      }

      let isActive
      async.doUntil(done => {
        this.isActive((err, _isActive) => {
          if (err) {
            return done(err)
          }

          isActive = _isActive
          done()
        })
      }, () => {
        return isActive
      }, callback)
    })
  }

  public isActive (callback) {
    this.hasStatus('ACTIVE', callback)
  }

  public isDeleting (callback) {
    this.hasStatus('DELETING', callback)
  }

  private hasStatus (status, callback) {
    this.describe(function (err, description) {
      if (err) {
        return callback(err)
      }

      const isActive = description.StreamDescription.StreamStatus === status
      callback(null, isActive)
    })
  }

  private describe (callback) {
    this.kinesis.describeStream({StreamName: this.name}, callback)
  }
}
