package dev.vortex.api.market;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * The only class that knows the Redis key layout (ADR 0017). Writes go through one Lua script,
 * so each trade updates price and candle atomically and duplicates are skipped inside Redis.
 */
@Component
public class CandleStore {

    private static final RedisScript<Long> APPLY_TRADE =
            RedisScript.of(new ClassPathResource("redis/apply_trade.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final long ttlSeconds;

    public CandleStore(StringRedisTemplate redis, @Value("${vortex.candle-ttl-hours}") long ttlHours) {
        this.redis = redis;
        this.ttlSeconds = Duration.ofHours(ttlHours).toSeconds();
    }

    // {SYM} is a hash tag: every key of one symbol maps to the same Redis Cluster slot
    static String lastTradeKey(String symbol) { return "trade:last:{" + symbol + "}"; }
    static String priceKey(String symbol) { return "price:{" + symbol + "}"; }
    static String candleKey(String symbol, long minute) { return "candle:{" + symbol + "}:1m:" + minute; }
    static String candleIndexKey(String symbol) { return "candles:{" + symbol + "}:1m"; }

    /** @return true if applied, false if Redis already had this trade (a re-read after a restart) */
    public boolean apply(TradeEvent trade) {
        long minute = trade.minuteEpochSeconds();
        Long applied = redis.execute(APPLY_TRADE,
                List.of(lastTradeKey(trade.symbol()), priceKey(trade.symbol()),
                        candleKey(trade.symbol(), minute), candleIndexKey(trade.symbol())),
                trade.engineRunId(), Long.toString(trade.matchId()), Long.toString(trade.priceTicks()),
                Long.toString(trade.qtyLots()), Long.toString(trade.eventTimeNs()),
                Long.toString(minute), Long.toString(ttlSeconds));
        return applied != null && applied == 1L;
    }

    public Optional<Map<String, String>> lastPrice(String symbol) {
        Map<Object, Object> raw = redis.opsForHash().entries(priceKey(symbol));
        return raw.isEmpty() ? Optional.empty() : Optional.of(strings(raw));
    }

    /** Up to `count` most recent candles, oldest first. */
    public List<Map<String, String>> recentCandles(String symbol, int count) {
        Set<String> minutes = redis.opsForZSet().reverseRange(candleIndexKey(symbol), 0, count - 1L);
        List<Map<String, String>> candles = new ArrayList<>();
        if (minutes == null) {
            return candles;
        }
        for (String minute : minutes) {
            Map<Object, Object> raw = redis.opsForHash().entries(candleKey(symbol, Long.parseLong(minute)));
            if (!raw.isEmpty()) {  // the index can briefly point at a candle that just expired
                candles.addFirst(strings(raw));
            }
        }
        return candles;
    }

    private static Map<String, String> strings(Map<Object, Object> raw) {
        Map<String, String> out = new java.util.TreeMap<>();
        raw.forEach((key, value) -> out.put(key.toString(), value.toString()));
        return out;
    }
}
