package dev.vortex.api.market;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Kafka "trades" -> Redis, one poll batch at a time.
 *
 * Trades are keyed by symbol, so each symbol lives in one partition and one listener thread
 * (concurrency = 3 = partitions): a symbol has exactly one writer. Each batch goes to Redis in
 * one pipeline; the offsets are committed after that (at least once), and the Lua script skips
 * trades it has already applied, so a re-read after a restart is not counted twice.
 */
@Component
public class TradeListener {

    private static final Logger log = LoggerFactory.getLogger(TradeListener.class);

    private final JsonMapper json = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)  // trade.v1 has more fields
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .build();
    private final CandleStore store;
    private final SymbolRegistry symbols;
    private final IngestStats stats;

    public TradeListener(CandleStore store, SymbolRegistry symbols, IngestStats stats) {
        this.store = store;
        this.symbols = symbols;
        this.stats = stats;
    }

    @KafkaListener(topics = "trades", batch = "true")
    public void onTrades(List<ConsumerRecord<String, String>> records) {
        List<TradeEvent> trades = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                TradeEvent trade = json.readValue(record.value(), TradeEvent.class);
                if (symbols.isKnown(trade.symbol())) {
                    trades.add(trade);
                }
                else {
                    stats.rejected();
                }
            }
            catch (JacksonException e) {
                // bad message: skip it, never block the partition (bronze keeps the raw message)
                stats.rejected();
                log.warn("skipping unreadable trade at {}-{}: {}", record.partition(), record.offset(), e.getOriginalMessage());
            }
        }
        if (trades.isEmpty()) {
            return;
        }

        long start = System.nanoTime();
        List<Boolean> applied = store.applyAll(trades);
        stats.redisBatch(System.nanoTime() - start, trades.size());

        for (int index = 0; index < trades.size(); ++index) {
            if (applied.get(index)) {
                stats.applied(trades.get(index).eventTimeNs());
            }
            else {
                stats.duplicate();
            }
        }
    }
}
