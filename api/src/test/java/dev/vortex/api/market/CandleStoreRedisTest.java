package dev.vortex.api.market;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** The Lua script against a real Redis 8 (Testcontainers): candle maths, duplicates, expiry. */
@Testcontainers
class CandleStoreRedisTest {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:8.8.2-alpine")).withExposedPorts(6379);

    private static final long MINUTE_NS = 60_000_000_000L;
    private static final long T0 = 29_890_500L * MINUTE_NS;  // start of some minute
    private static final String RUN = "9f3c2a71b4e08d56";

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private CandleStore store;

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379)));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void disconnect() {
        factory.destroy();
    }

    @BeforeEach
    void cleanRedis() {
        redis.getConnectionFactory().getConnection().serverCommands().flushAll();
        store = new CandleStore(redis, 24);
    }

    private static TradeEvent trade(String run, long match, long price, long qty, long eventTimeNs) {
        return new TradeEvent(run, "AAPL", match, price, qty, "BUY", eventTimeNs);
    }

    @Test
    void tradesInOneMinuteBuildOneCandle() {
        store.apply(trade(RUN, 1, 19000, 10, T0 + 1));
        store.apply(trade(RUN, 2, 19010, 5, T0 + 2));
        store.apply(trade(RUN, 3, 18990, 7, T0 + 3));
        store.apply(trade(RUN, 4, 19005, 3, T0 + 4));

        List<Map<String, String>> candles = store.recentCandles("AAPL", 10);
        assertThat(candles).hasSize(1);
        Map<String, String> c = candles.getFirst();
        assertThat(c).containsEntry("open", "19000").containsEntry("high", "19010")
                .containsEntry("low", "18990").containsEntry("close", "19005")
                .containsEntry("volume_lots", "25").containsEntry("trades", "4");
        assertThat(store.lastPrice("AAPL")).get().extracting(p -> p.get("price_ticks")).isEqualTo("19005");
    }

    @Test
    void nextMinuteStartsANewCandleAndReadsComeOldestFirst() {
        store.apply(trade(RUN, 1, 19000, 1, T0));
        store.apply(trade(RUN, 2, 19020, 1, T0 + MINUTE_NS));

        List<Map<String, String>> candles = store.recentCandles("AAPL", 10);
        assertThat(candles).extracting(c -> c.get("open")).containsExactly("19000", "19020");
        assertThat(store.recentCandles("AAPL", 1)).extracting(c -> c.get("open")).containsExactly("19020");
    }

    @Test
    void aTradeSeenTwiceIsCountedOnce() {
        assertThat(store.apply(trade(RUN, 1, 19000, 10, T0))).isTrue();
        assertThat(store.apply(trade(RUN, 2, 19010, 10, T0 + 1))).isTrue();
        // a restart re-reads trades 1 and 2 from Kafka
        assertThat(store.apply(trade(RUN, 1, 19000, 10, T0))).isFalse();
        assertThat(store.apply(trade(RUN, 2, 19010, 10, T0 + 1))).isFalse();

        assertThat(store.recentCandles("AAPL", 1).getFirst())
                .containsEntry("volume_lots", "20").containsEntry("trades", "2");
    }

    @Test
    void aNewEngineRunStartsMatchIdsAgainAndIsNotADuplicate() {
        store.apply(trade(RUN, 50, 19000, 1, T0));
        assertThat(store.apply(trade("0000000000000001", 1, 19001, 1, T0 + 1))).isTrue();
        assertThat(store.recentCandles("AAPL", 1).getFirst()).containsEntry("trades", "2");
    }

    @Test
    void candlesExpire() {
        store.apply(trade(RUN, 1, 19000, 1, T0));
        Long ttl = redis.getExpire(CandleStore.candleKey("AAPL", T0 / 1_000_000_000L));
        assertThat(ttl).isBetween(24 * 3600L - 5, 24 * 3600L);
    }
}
