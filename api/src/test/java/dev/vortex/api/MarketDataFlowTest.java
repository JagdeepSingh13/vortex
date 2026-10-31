package dev.vortex.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The whole service against real Kafka and Redis (Testcontainers): trade.v1 messages on the
 * "trades" topic -> candles and price over HTTP, plus a duplicate and an unreadable message.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",  // the test produces before assignment
        "vortex.symbols-file=../config/symbols.yaml",
})
class MarketDataFlowTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

    @Container
    @ServiceConnection(name = "redis")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:8.8.2-alpine")).withExposedPorts(6379);

    @Autowired
    KafkaTemplate<String, String> kafka;

    @LocalServerPort
    int port;

    private static String trade(long match, long priceTicks, long qty, long eventTimeNs) {
        return """
                {"schema_version":1,"engine_run_id":"9f3c2a71b4e08d56","match_id":%d,"symbol":"AAPL",\
                "price_ticks":%d,"qty_lots":%d,"buy_participant_id":7,"buy_order_id":1,\
                "sell_participant_id":8,"sell_order_id":2,"aggressor_side":"BUY",\
                "sequence_number":%d,"event_time_ns":%d}""".formatted(match, priceTicks, qty, match, eventTimeNs);
    }

    @Test
    void tradesOnKafkaBecomeCandlesAndAPriceOverHttp() throws Exception {
        long now = Instant.now().toEpochMilli() * 1_000_000L;
        long minuteStart = Math.floorDiv(now, 60_000_000_000L) * 60_000_000_000L;
        kafka.send("trades", "AAPL", trade(1, 19000, 10, minuteStart + 1));
        kafka.send("trades", "AAPL", trade(2, 19010, 5, minuteStart + 2));
        kafka.send("trades", "AAPL", "this is not json");
        kafka.send("trades", "AAPL", trade(2, 19010, 5, minuteStart + 2));  // duplicate
        kafka.send("trades", "AAPL", trade(3, 18990, 5, minuteStart + 3)).get();

        RestClient http = RestClient.create("http://localhost:" + port);
        List<Map<String, Object>> candles = List.of();
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            candles = http.get().uri("/candles/AAPL?minutes=5").retrieve().body(new ParameterizedTypeReference<>() {});
            if (!candles.isEmpty() && "3".equals(String.valueOf(candles.getFirst().get("trades")))) {
                break;
            }
            Thread.sleep(200);
        }

        assertThat(candles).hasSize(1);
        assertThat(candles.getFirst())
                .containsEntry("open", "190.00").containsEntry("high", "190.10")
                .containsEntry("low", "189.90").containsEntry("close", "189.90");
        assertThat(String.valueOf(candles.getFirst().get("volume_lots"))).isEqualTo("20");

        Map<String, Object> price = http.get().uri("/prices/AAPL").retrieve().body(new ParameterizedTypeReference<>() {});
        assertThat(price).containsEntry("price", "189.90");

        Map<String, Object> stats = http.get().uri("/stats").retrieve().body(new ParameterizedTypeReference<>() {});
        assertThat(stats).containsEntry("applied", 3).containsEntry("duplicates", 1).containsEntry("rejected", 1);

        // retrieve() would throw on 404; exchange() hands back the status itself
        int unknown = http.get().uri("/prices/GOOG").exchange((request, response) -> response.getStatusCode().value());
        assertThat(unknown).isEqualTo(404);
    }
}
