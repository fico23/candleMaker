const fetch = require('node-fetch')
const { TickerStream } = require('node-bitstamp')
const Binance = require('binance-api-node').default
const hitbtcClient = require('./exchanges/hitbtc')
const redisClient = require('./util/redisClient')

const binanceClient = Binance()

const tradesQueue = []

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

const processTrades = async () => {
  if (tradesQueue.length === 0) {
    console.log('no trade, wait 0.1 sec')
    await delay(100)
    processTrades()
    return
  }
  console.log(`process ${tradesQueue.length} trades`)
  const start = Date.now()
  await redisClient.processTrades(tradesQueue.splice(0, tradesQueue.length))
  console.log(`finished in ${(Date.now() - start) / 1000} seconds`)
  processTrades()
}

const bitstampPairsUrl = 'https://www.bitstamp.net/api/v2/trading-pairs-info/'
const getBitstampPairs = async () => {
  const bitstampResponse = await fetch(bitstampPairsUrl)
  const bitstampPairs = await bitstampResponse.json()

  return bitstampPairs.map(x => { return { symbol: x.url_symbol, name: x.name } })
}

const subscribeBitstampTrades = async () => {
  const bitstampPairs = await getBitstampPairs()

  const bitstampTradeStream = new TickerStream()
  bitstampTradeStream.on('connected', () => {
    console.log('bitstamp connected')

    for (const pair of bitstampPairs) {
      const tickerTopic = bitstampTradeStream.subscribe(pair.symbol)

      /* e.g.
          {
              amount: 0.01513062,
              buy_order_id: 297260696,
              sell_order_id: 297260910,
              amount_str: '0.01513062',
              price_str: '212.80',
              timestamp: '1505558814',
              price: 212.8,
              type: 1,
              id: 21565524,
              cost: 3.219795936
          }
      */
      bitstampTradeStream.on(tickerTopic, data => {
        data.market = `bitstamp:${pair.name}`
        data.timestamp = data.microtimestamp
        tradesQueue.push(data)
      })
    }
  })

  bitstampTradeStream.on('disconnected', () => {
    console.log('bitstamp disconnected')
  })
}

/* e.g.
    {
      eventType: 'trade',
      eventTime: 1508614495052,
      symbol: 'ETHBTC',
      price: '0.04923600',
      quantity: '3.43500000',
      maker: false,
      tradeId: 2148226
    }
*/
const bitstampifyBinanceTrade = (trade) => {
  return {
    amount: trade.quantity,
    price: trade.price,
    timestamp: trade.eventTime,
    id: trade.tradeId,
    type: 1,
    symbol: trade.symbol
  }
}

const getBinancePairs = async () => {
  const binanceInfo = await binanceClient.exchangeInfo()

  const symbolsDictionary = {}
  const subscribePairs = []
  for (const pair of binanceInfo.symbols) {
    subscribePairs.push(pair.symbol)
    symbolsDictionary[pair.symbol] = `${pair.baseAsset}/${pair.quoteAsset}`
  }

  return { subscribePairs, symbolsDictionary }
}

const subscribeBinanceTrades = async () => {
  const { subscribePairs, symbolsDictionary } = await getBinancePairs()

  binanceClient.ws.trades(subscribePairs, binanceTrade => {
    const trade = bitstampifyBinanceTrade(binanceTrade)

    trade.market = `binance:${symbolsDictionary[trade.symbol]}`

    tradesQueue.push(trade)
  })
}

const bitstampifyHitbtcTrade = (trade, symbol) => {
  const tradeDate = new Date(trade.timestamp)
  return {
    amount: trade.quantity,
    price: trade.price,
    timestamp: tradeDate.getTime(),
    id: trade.id,
    type: trade.side === 'buy' ? 1 : 0,
    symbol: symbol
  }
}

const subscribeHitbtcTrades = async () => {
  const socketApi = await hitbtcClient.connectSocket()

  socketApi.setHandler('updateTrades', trades => {
    for (const hitbtcTrade of trades.data) {
      const trade = bitstampifyHitbtcTrade(hitbtcTrade, trades.symbol)

      trade.market = `hitbtc:${hitbtcClient.getSymbolName([trades.symbol])}`

      tradesQueue.push(trade)
    }
  })
}

(async () => {
  await subscribeBitstampTrades()
  await subscribeBinanceTrades()
  await subscribeHitbtcTrades()

  await redisClient.load()

  processTrades()
})()
