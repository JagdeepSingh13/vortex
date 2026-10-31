-- Apply one trade to the live market data of its symbol (ADR 0017).
--
-- Runs inside Redis: no other command can run in the middle, so nobody ever sees a half-updated
-- candle, and the whole update is one network round trip.
--
-- KEYS[1]  trade:last:{SYM}           last trade applied: engine run id + match id (duplicate check)
-- KEYS[2]  price:{SYM}                last traded price
-- KEYS[3]  candle:{SYM}:1m:<minute>   the 1-minute candle the trade belongs to (by engine event time)
-- KEYS[4]  candles:{SYM}:1m           sorted set of candle minutes, for "last N candles" reads
--
-- ARGV: engine_run_id, match_id, price_ticks, qty_lots, event_time_ns, minute (epoch seconds),
--       ttl seconds
--
-- {SYM} in every key is a Redis Cluster hash tag: all keys of one symbol live on the same node,
-- which a multi-key script requires there.
--
-- Returns 1 if applied, 0 if the trade was a duplicate (already applied before a restart).

local run, match = ARGV[1], tonumber(ARGV[2])
local price, qty = ARGV[3], ARGV[4]
local event_time_ns, minute, ttl = ARGV[5], tonumber(ARGV[6]), tonumber(ARGV[7])

-- 1. duplicate check: within one engine run, match ids only grow per symbol
local last = redis.call('HMGET', KEYS[1], 'run', 'match')
if last[1] == run and tonumber(last[2]) >= match then
    return 0
end
redis.call('HSET', KEYS[1], 'run', run, 'match', ARGV[2])

-- 2. last price
redis.call('HSET', KEYS[2], 'price_ticks', price, 'qty_lots', qty,
    'event_time_ns', event_time_ns, 'engine_run_id', run, 'match_id', ARGV[2])

-- 3. candle. Prices are integers up to 2^53 (trade.v1), exact in Lua's double numbers.
local p = tonumber(price)
if redis.call('EXISTS', KEYS[3]) == 0 then
    redis.call('HSET', KEYS[3], 'minute', minute, 'open', price, 'high', price, 'low', price, 'close', price)
else
    local hl = redis.call('HMGET', KEYS[3], 'high', 'low')
    if p > tonumber(hl[1]) then redis.call('HSET', KEYS[3], 'high', price) end
    if p < tonumber(hl[2]) then redis.call('HSET', KEYS[3], 'low', price) end
    redis.call('HSET', KEYS[3], 'close', price)
end
redis.call('HINCRBY', KEYS[3], 'volume_lots', qty)
redis.call('HINCRBY', KEYS[3], 'trades', 1)
redis.call('EXPIRE', KEYS[3], ttl)

-- 4. index of minutes, trimmed to the retention window
redis.call('ZADD', KEYS[4], minute, minute)
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', minute - ttl)
redis.call('EXPIRE', KEYS[4], ttl)

return 1
