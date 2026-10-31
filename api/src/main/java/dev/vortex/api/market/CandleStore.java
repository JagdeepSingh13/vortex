package dev.vortex.api.market;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * The only class that knows the Redis key layout (ADR 0017). Writes go through one Lua script,
 * so each trade updates price and candle atomically and duplicates are skipped inside Redis.
 */
@Component
public class CandleStore {

    // the script text is read once. RedisScript.of(Resource) re-checks on every call whether the
    // resource changed (a synchronized check that opens the jar entry): measured at ~6.8 ms per
    // trade and it serialised all listener threads. A String script has no such check.
    private static final RedisScript<Long> APPLY_TRADE = RedisScript.of(readScript("redis/apply_trade.lua"), Long.class);

    private static String readScript(String path) {
        try (var in = new ClassPathResource(path).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException("cannot read " + path, e);
        }
    }

    private final StringRedisTemplate redis;
    private final long ttlSeconds;

    private volatile String scriptSha;

    public CandleStore(StringRedisTemplate redis, @Value("${vortex.candle-ttl-hours}") long ttlHours) {
        this.redis = redis;
        this.ttlSeconds = Duration.ofHours(ttlHours).toSeconds();
        this.scriptSha = loadScript();  // SCRIPT LOAD once; afterwards calls send only the SHA
    }

    // {SYM} is a hash tag: every key of one symbol maps to the same Redis Cluster slot
    static String lastTradeKey(String symbol) { return "trade:last:{" + symbol + "}"; }
    static String priceKey(String symbol) { return "price:{" + symbol + "}"; }
    static String candleKey(String symbol, long minute) { return "candle:{" + symbol + "}:1m:" + minute; }
    static String candleIndexKey(String symbol) { return "candles:{" + symbol + "}:1m"; }

    /** @return true if applied, false if Redis already had this trade (a re-read after a restart) */
    public boolean apply(TradeEvent trade) {
        return applyAll(List.of(trade)).getFirst();
    }

    /**
     * Applies a batch of trades with one network round trip: every trade is its own atomic
     * script call (EVALSHA), all sent in one pipeline. Redis runs pipelined commands in order,
     * so per-symbol order is kept.
     *
     * Measured: one call per trade spent most of its time waiting for the network; Redis's own
     * benchmark showed ~5-7k sequential requests/s versus ~260-280k/s pipelined.
     *
     * @return per trade: true if applied, false if it was a duplicate
     */
    public List<Boolean> applyAll(List<TradeEvent> trades) {
        try {
            return pipelined(trades);
        }
        catch (DataAccessException e) {
            // Redis restarted and forgot the script (NOSCRIPT): load it again and retry the
            // batch. Safe: trades already applied are recognised as duplicates.
            scriptSha = loadScript();
            return pipelined(trades);
        }
    }

    private List<Boolean> pipelined(List<TradeEvent> trades) {
        String sha = scriptSha;
        List<Object> results = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (TradeEvent trade : trades) {
                connection.scriptingCommands().evalSha(sha, ReturnType.INTEGER, 4, keysAndArgs(trade));
            }
            return null;
        });
        List<Boolean> applied = new ArrayList<>(results.size());
        for (Object result : results) {
            applied.add(result instanceof Long value && value == 1L);
        }
        return applied;
    }

    private byte[][] keysAndArgs(TradeEvent trade) {
        long minute = trade.minuteEpochSeconds();
        String symbol = trade.symbol();
        String[] values = {
                lastTradeKey(symbol), priceKey(symbol), candleKey(symbol, minute), candleIndexKey(symbol),
                trade.engineRunId(), Long.toString(trade.matchId()), Long.toString(trade.priceTicks()),
                Long.toString(trade.qtyLots()), Long.toString(trade.eventTimeNs()),
                Long.toString(minute), Long.toString(ttlSeconds),
        };
        byte[][] bytes = new byte[values.length][];
        for (int index = 0; index < values.length; ++index) {
            bytes[index] = values[index].getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    private String loadScript() {
        return redis.execute((RedisCallback<String>) connection ->
                connection.scriptingCommands().scriptLoad(APPLY_TRADE.getScriptAsString().getBytes(StandardCharsets.UTF_8)));
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
