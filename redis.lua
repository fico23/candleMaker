local candleSizesMinutes = { 1, 5, 15, 30, 60, 120, 240, 360, 720, 1440 };
local candleSizesMiliseconds = { 60000, 300000, 900000, 1800000, 3600000, 7200000, 14400000, 21600000, 43200000, 86400000 };
local maxCandleNumber = 1440;

local function candleConstructor(candle, updatedAt)
    return {
        open = candle.open,
        high = candle.high,
        close = candle.close,
        low = candle.low,
        volume = candle.volume,
        tradesCounter = candle.tradesCounter,
        timestamp = candle.timestamp,
        market = candle.market,
        size = candle.size,
        updatedAt = updatedAt
    };
end

local function emptyCandleConstructor(open, market, timestamp, updatedAt, size)
    return {
        open = open,
        high = open,
        close = open,
        low = open,
        volume = 0,
        tradesCounter = 0,
        timestamp = timestamp,
        market = market,
        updatedAt = updatedAt,
        size = size
    };
end

local function saveTrade(market, id, price, amount, timestamp, recievedAt, type)
    local tradeKey = 'trade:' .. market .. id;
    local tradesKey = 'trades:' .. market;

    redis.call('HSET', tradeKey, 'market', market);
    redis.call('HSET', tradeKey, 'id', id);
    redis.call('HSET', tradeKey, 'price', price);
    redis.call('HSET', tradeKey, 'amount', amount);
    redis.call('HSET', tradeKey, 'price', price);
    redis.call('HSET', tradeKey, 'timestamp', timestamp);
    redis.call('HSET', tradeKey, 'recievedAt', recievedAt);
    redis.call('HSET', tradeKey, 'type', type);

    redis.call('ZADD', tradesKey, timestamp, id);
end

local function getCandleProperty(stringifiedCandle, property)
    local reg = property .. "\":(.-),";
    local propertyValue = string.match(stringifiedCandle, reg);

    return propertyValue;
end

local function setCandleProperty(stringifiedCandle, property, value)
    local newCandle = candle:gsub(property .. "\":(.-),", property .. "\":" .. value .. ",");

    return newCandle;
end

local function updateCandle(candle, recievedAt)
    local candlesKey = 'candles:' .. candle.market .. '_' .. candle.size;

    local lastCandle = redis.call('ZPOPMAX', candlesKey);

    if (#lastCandle == 0) then
        local newCandle = candleConstructor(candle, recievedAt);
        redis.call('ZADD', candlesKey, newCandle.timestamp, cjson.encode(newCandle));
        return;
    end

    local lastCandleTimestamp = lastCandle[2];
    local lastCandleTable = cjson.decode(lastCandle[1]);

    if lastCandleTimestamp == candle.timestamp then
        lastCandleTable.tradesCounter = lastCandleTable.tradesCounter + candle.tradesCounter;
        lastCandleTable.volume = lastCandleTable.volume + candle.volume;
        lastCandleTable.close = lastCandleTable.close;

        if lastCandleTable.high < candle.high then
            lastCandleTable.high = candle.high;
        end

        if lastCandleTable.low > candle.low then
            lastCandleTable.low = candle.low;
        end
    end

    redis.call('ZADD', candlesKey, lastCandleTable.timestamp, cjson.encode(lastCandleTable));

    local candleSizeMiliseconds =  candle.size * 60000;
    local currentCandleTimestamp = lastCandleTimestamp + candleSizeMiliseconds;

    while candle.timestamp >= currentCandleTimestamp do
        local newCandle = {};

        
        if candle.timestamp == currentCandleTimestamp then
            newCandle = candleConstructor(candle, recievedAt);
            newCandle.open = lastCandle.close;
        else
            newCandle = emptyCandleConstructor(lastCandle.close, candle.market, currentCandleTimestamp, recievedAt, candle.size)
        end

        
        redis.call('ZADD', candlesKey, currentCandleTimestamp, cjson.encode(newCandle));

        currentCandleTimestamp = currentCandleTimestamp + candleSizeMiliseconds;
    end

    local candleCount = redis.call('ZCARD', candlesKey);
    if candleCount > maxCandleNumber then
        redis.call('ZPOPMIN', candlesKey, candleCount - maxCandleNumber);
    end
end

local function prepareCandleUpdate(trades)
    local updateCandles = {};

    for i = 1, #trades do
        local trade = trades[i];

        for j = 1, #candleSizesMinutes do
            local candleSizeMinutes = candleSizesMinutes[j];
            local candleSizeMiliseconds = candleSizesMiliseconds[j];
            local tradeCandleSizeTimestamp = math.floor(trade.timestamp / candleSizeMiliseconds) * candleSizeMiliseconds;
            local candleKey = trade.market .. "_" .. candleSizeMinutes .. "_" .. tradeCandleSizeTimestamp;


            if updateCandles[candleKey] == nil then
                updateCandles[candleKey] = {
                    market = trade.market,
                    size = candleSizeMinutes,
                    tradesCounter = 1,
                    volume = trade.amount,
                    high = trade.price,
                    low = trade.price,
                    open = trade.price,
                    close = trade.price,
                    timestamp = tradeCandleSizeTimestamp
                };
            else 
                updateCandles[candleKey].tradesCounter = updateCandles[candleKey].tradesCounter + 1;
                updateCandles[candleKey].volume = updateCandles[candleKey].volume + trade.amount;
                updateCandles[candleKey].close = trade.price;

                if updateCandles[candleKey].high < trade.price then
                    updateCandles[candleKey].high = trade.price;
                end

                if updateCandles[candleKey].low > trade.price then
                    updateCandles[candleKey].low = trade.price;
                end
            end
        end
    end

    return updateCandles;
end



local function updateCandles(trades, recievedAt)
    local candleUpdates = prepareCandleUpdate(trades);

    for key, value in pairs(candleUpdates) do
        updateCandle(value, recievedAt);
    end
end

local function processTrades(stringifiedTrades, recievedAt)
    local trades = cjson.decode(stringifiedTrades);
    for i = 1, #trades do
        local trade = trades[i];
        saveTrade(trade.market, trade.id, trade.price, trade.amount, trade.timestamp, recievedAt, trade.type);
    end

    updateCandles(trades, recievedAt);
end

local function saveOrderbookSnapshot(snapshot)

end

local function saveOrderbookPatch(patch)

end

return {
    saveOrderbookSnapshot = saveOrderbookSnapshot,
    saveOrderbookPatch = saveOrderbookPatch,
    processTrades = processTrades
};
