const WebSocket = require('ws')

const logger = {
  debug: (...arg) => {
    // console.log((new Date).toISOString(), 'DEBUG', ...arg)
  },
  info: (...arg) => {
    console.log((new Date()).toISOString(), 'INFO', ...arg)
  },
  warn: (...arg) => {
    console.log((new Date()).toISOString(), 'WARN', ...arg)
  }
}

const symbolsDictionary = {}

class SocketClient {
  constructor (onConnected) {
    this._id = 1
    this._createSocket()
    this._onConnected = onConnected
    this._promises = new Map()
    this._handles = new Map()
  }

  _createSocket () {
    this._ws = new WebSocket('wss://api.hitbtc.com/api/2/ws')
    this._ws.onopen = () => {
      logger.info('ws connected')
      this._onConnected()
    }
    this._ws.onclose = () => {
      logger.warn('ws closed')
      this._promises.forEach((cb, id) => {
        this._promises.delete(id)
        cb.reject(new Error('Disconnected'))
      })
      setTimeout(() => this._createSocket(), 500)
    }
    this._ws.onerror = err => {
      logger.warn('ws error', err)
    }
    this._ws.onmessage = msg => {
      logger.debug('<', msg.data)
      try {
        const message = JSON.parse(msg.data)
        if (message.id) {
          if (this._promises.has(message.id)) {
            const cb = this._promises.get(message.id)
            this._promises.delete(message.id)
            if (message.result) {
              cb.resolve(message.result)
            } else if (message.error) {
              cb.reject(message.error)
            } else {
              logger.warn('Unprocessed response', message)
            }
          }
        } else if (message.method && message.params) {
          if (this._handles.has(message.method)) {
            this._handles.get(message.method).forEach(cb => {
              cb(message.params)
            })
          } else {
            logger.debug('Unprocessed method', message)
          }
        } else {
          logger.warn('Unprocessed message', message)
        }
      } catch (e) {
        logger.warn('Fail parse message', e)
      }
    }
  }

  request (method, params = {}) {
    if (this._ws.readyState === WebSocket.OPEN) {
      return new Promise((resolve, reject) => {
        const requestId = ++this._id
        this._promises.set(requestId, { resolve, reject })
        const msg = JSON.stringify({ method, params, id: requestId })
        logger.debug('>', msg)
        this._ws.send(msg)
        setTimeout(() => {
          if (this._promises.has(requestId)) {
            this._promises.delete(requestId)
            reject(new Error('Timeout'))
          }
        }, 10000)
      })
    } else {
      return Promise.reject(new Error('WebSocket connection not established'))
    }
  }

  setHandler (method, callback) {
    if (!this._handles.has(method)) {
      this._handles.set(method, [])
    }
    this._handles.get(method).push(callback)
  }
}

module.exports = {
  connectSocket: async () => {
    const socketApi = new SocketClient(async () => {
      try {
        const symbols = await socketApi.request('getSymbols')

        for (const s of symbols) {
          symbolsDictionary[s.id] = `${s.baseCurrency}/${s.quoteCurrency}`
          await socketApi.request('subscribeTrades', { symbol: s.id })
        }
      } catch (e) {
        logger.warn(e)
      }
    })
    return socketApi
  },
  getSymbolName: (symbol) => {
    return symbolsDictionary[symbol]
  }

}
