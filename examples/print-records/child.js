var kcl = require('../../')

kcl.AbstractConsumer.extend({
  processRecords: function (records, done) {
    records.forEach(function (r) {
      console.log('Partition key: %s, Sequence number: %s', r.PartitionKey, r.SequenceNumber)
      console.log('Data: %s', r.Data.toString())
      console.log()
    })

    var lastRecord = records.pop()
    if (lastRecord) {
      console.log('Calling done with `true` to checkpoint at: %s', lastRecord.SequenceNumber)
    }
    done(null, true)
  }
})
