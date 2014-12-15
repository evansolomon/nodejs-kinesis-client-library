/**
 * @fileoverview Manage HTTP server.
 */

var http = require('http')

/**
 * Create an HTTP server.
 *
 * @param  {(number|string)}  port
 * @param  {Function}         callback  Function to respond to HTTP requests.
 */
module.exports.create = function (port, callback) {
  var server = http.createServer(function (req, res) {
    try {
      var response = callback()
      res.writeHead(200, {'Content-Type': 'text/plain'})
      res.end(response.toString())
    } catch (e) {
      res.writeHead(500, {'Content-Type': 'text/plain'})
      res.end(e.toString())
    }
  })

  server.listen(port)
}
