import * as http from 'http'

const responseHeaders = {'Content-Type': 'text/plain'}

const create = (port: number|string, callback: () => Stringable) => {
  const server = http.createServer((req, res) => {
    try {
      const response = callback()
      res.writeHead(200, responseHeaders)
      res.end(response.toString())
    } catch (e) {
      res.writeHead(500, responseHeaders)
      res.end(e.toString())
    }
  })

  server.listen(port)
}

export interface Stringable {
  toString: () => string
}

export {create}
