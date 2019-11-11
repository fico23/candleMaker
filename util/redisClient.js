const fs = require('fs')
const { promisify } = require('util')
const redis = require('redis')
const redisClient = redis.createClient({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT,
  password: process.env.REDIS_PASSWORD
})

const script = promisify(redisClient.script).bind(redisClient)
const evalsha = promisify(redisClient.evalsha).bind(redisClient)

const sha1FromScript = (script) => {
  return require('crypto').createHash('sha1').update(script).digest('hex')
}

let processTradesSHA, saveOrderbookSnapshotSHA, saveOrderbookPatchSHA

module.exports = {
  load: async () => {
    const exchangeSHA = await script('load', fs.readFileSync('./redis.lua'))

    const processTradesScript = `local e = f_${exchangeSHA}(); return e.processTrades(ARGV[1], ARGV[2]);`
    processTradesSHA = sha1FromScript(processTradesScript)
    const saveOrderbookSnapshotScript = `local e = f_${exchangeSHA}(); return e.saveOrderbookSnapshot(ARGV[1], ARGV[2], ARGV[3]);`
    saveOrderbookSnapshotSHA = sha1FromScript(saveOrderbookSnapshotScript)
    const saveOrderbookPatchScript = `local e = f_${exchangeSHA}(); return e.saveOrderbookPatch(ARGV[1], ARGV[2], ARGV[3]);`
    saveOrderbookPatchSHA = sha1FromScript(saveOrderbookPatchScript)

    await script('load', processTradesScript)
    await script('load', saveOrderbookSnapshotScript)
    await script('load', saveOrderbookPatchScript)

    console.log('Lua script loaded into Redis')
  },

  processTrades: async (trades) => {
    const res = await evalsha(processTradesSHA, 0, JSON.stringify(trades), Date.now())
    return res
  },

  saveOrderbookSnapshot: async (asks, bids, timestamp) => {
    const res = await evalsha(saveOrderbookSnapshotSHA, 0, asks, bids, timestamp)
    return res
  },

  saveOrderbookPatch: async (asks, bids, timestamp) => {
    const res = await evalsha(saveOrderbookPatchSHA, 0, asks, bids, timestamp)
    return res
  }
}
