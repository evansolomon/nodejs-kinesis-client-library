var kcl = require('../../../')
kcl.AbstractConsumer.extend({
  processRecords: function (records) {
    records.forEach(function (record) {
      console.log('Line:', record.Data.toString())
    })
  }
})
