package dev.vortex.api.market;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read side of live market data. Prices are returned both as integer ticks and as a decimal
 * string (exact, from the tick size); a decimal is never a JSON float.
 */
@RestController
public class MarketDataController {

    private final CandleStore store;
    private final SymbolRegistry symbols;
    private final IngestStats stats;

    public MarketDataController(CandleStore store, SymbolRegistry symbols, IngestStats stats) {
        this.store = store;
        this.symbols = symbols;
        this.stats = stats;
    }

    @GetMapping("/symbols")
    public Object symbols() {
        return symbols.symbols();
    }

    @GetMapping("/prices/{symbol}")
    public ResponseEntity<Map<String, Object>> price(@PathVariable String symbol) {
        if (!symbols.isKnown(symbol)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return store.lastPrice(symbol)
                .map(raw -> {
                    Map<String, Object> body = new LinkedHashMap<>();
                    long ticks = Long.parseLong(raw.get("price_ticks"));
                    body.put("symbol", symbol);
                    body.put("price", symbols.price(symbol, ticks));
                    body.put("price_ticks", ticks);
                    body.put("qty_lots", Long.parseLong(raw.get("qty_lots")));
                    body.put("event_time_ns", raw.get("event_time_ns"));  // string: exceeds 2^53
                    body.put("engine_run_id", raw.get("engine_run_id"));
                    body.put("match_id", Long.parseLong(raw.get("match_id")));
                    return ResponseEntity.ok(body);
                })
                .orElse(ResponseEntity.noContent().build());  // known symbol, no trade yet
    }

    @GetMapping("/candles/{symbol}")
    public ResponseEntity<List<Map<String, Object>>> candles(@PathVariable String symbol,
                                                             @RequestParam(defaultValue = "30") int minutes) {
        if (!symbols.isKnown(symbol)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        int count = Math.clamp(minutes, 1, 24 * 60);
        List<Map<String, Object>> body = store.recentCandles(symbol, count).stream().map(raw -> {
            Map<String, Object> candle = new LinkedHashMap<>();
            candle.put("minute", Long.parseLong(raw.get("minute")));
            for (String field : List.of("open", "high", "low", "close")) {
                candle.put(field, symbols.price(symbol, Long.parseLong(raw.get(field))));
            }
            candle.put("volume_lots", Long.parseLong(raw.get("volume_lots")));
            candle.put("trades", Long.parseLong(raw.get("trades")));
            return candle;
        }).toList();
        return ResponseEntity.ok(body);
    }

    @GetMapping("/stats")
    public IngestStats.Snapshot stats() {
        return stats.snapshot();
    }

    /** Clears the counters and latency samples, so a benchmark measures only its own run. */
    @PostMapping("/stats/reset")
    public ResponseEntity<Void> resetStats() {
        stats.reset();
        return ResponseEntity.noContent().build();
    }
}
