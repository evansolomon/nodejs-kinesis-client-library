# Node Kinesis Client Library

Based on AWS' [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client), reimplemented in Node.js.

* Build Kinesis consumers easily
* Automatically scale up or down as streams split or merge
* Allow distributed processing without any extra code


## Usage

Call `kcl.AbstractConsumer.extend` with an object that implements some/all of these methods:

* `processRecords` (required): Accepts an array of [Record](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html) objects and a callback. Pass `true` as the second argument to the callback when you want to save a checkpoint — i.e. when you have processed a chunk of data.
* `initialize` (optional): Called when a consumer is spawned, before any records are processed. Accepts a callback that must be called to start record processing.
* `shutdown` (optional): Called when a consumer is about to exit. Accepts a callback that must be called to complete shutdown; if the callback is not called without 30 seconds the process exits anyway.

```js
var kcl = require('kinesis-client-library')
kcl.AbstractConsumer.extend({
  initialize: function (done) {
    this.cachedRecords = []
    this.cachedRecordsSize = 0
  },

  processRecords: function (records, done) {
    records.forEach(function (record) {
      this.cachedRecordsSize += record.length
      this.cachedRecords.push(record.Data)
    }.bind(this))

    var shouldCheckpoint = this.cachedRecordsSize > 50000000
    if (! shouldCheckpoint) return done()

    uploadCachedRecords(this.cachedRecords, function (err) {
      this.cachedRecords = []
      this.cachedRecordsSize = 0

      if (err) return done(err)
      done(null, true)
    }.bind(this))
  }
})
```
