import http = require('http')

export var create = function (port: number|string, callback: () => Stringable) {
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

export interface Stringable {
  toString: () => string;
}
