package dev.vortex.api.viewer;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Terminal candlestick chart of the live 1-minute candles (M8, ADR 0017).
 *
 *   java ... dev.vortex.api.viewer.CandleViewer [SYMBOL] [refresh seconds] [api url]
 *
 * Reads GET /candles/{symbol} from the market data service (it never touches Redis itself, so
 * only CandleStore knows the key layout) and redraws with plain ANSI escape codes: no terminal
 * library. Green = close >= open, red = close < open; the thin line is the high-low range.
 * Ctrl+C quits.
 */
public final class CandleViewer {

    private static final int CHART_ROWS = 20;
    private static final int MAX_CANDLES = 40;
    private static final String GREEN = "\u001b[32m";
    private static final String RED = "\u001b[31m";
    private static final String DIM = "\u001b[2m";
    private static final String BOLD = "\u001b[1m";
    private static final String RESET = "\u001b[0m";
    private static final String CLEAR = "\u001b[H\u001b[2J";
    private static final DateTimeFormatter HHMM = DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneOffset.UTC);

    private CandleViewer() {
    }

    record Candle(long minute, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close,
                  long volumeLots, long trades) {
    }

    public static void main(String[] args) throws Exception {
        String symbol = args.length > 0 ? args[0] : "AAPL";
        int refreshSeconds = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String api = args.length > 2 ? args[2] : "http://localhost:8090";

        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        JsonMapper json = JsonMapper.builder().build();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.print(RESET + "\n")));

        while (true) {
            String screen;
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(api + "/candles/" + symbol + "?minutes=" + MAX_CANDLES))
                        .timeout(Duration.ofSeconds(2)).build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 404) {
                    screen = "unknown symbol " + symbol + "\n";
                }
                else {
                    List<Map<String, Object>> raw = json.readValue(response.body(), new TypeReference<>() {});
                    screen = render(symbol, raw.stream().map(CandleViewer::candle).toList(), refreshSeconds);
                }
            }
            catch (java.io.IOException e) {
                screen = "cannot reach " + api + " (" + e.getMessage() + "); is `make api-run` running?\n";
            }
            System.out.print(CLEAR + screen);
            System.out.flush();
            Thread.sleep(refreshSeconds * 1000L);
        }
    }

    private static Candle candle(Map<String, Object> raw) {
        return new Candle(((Number) raw.get("minute")).longValue(),
                new BigDecimal((String) raw.get("open")), new BigDecimal((String) raw.get("high")),
                new BigDecimal((String) raw.get("low")), new BigDecimal((String) raw.get("close")),
                ((Number) raw.get("volume_lots")).longValue(), ((Number) raw.get("trades")).longValue());
    }

    static String render(String symbol, List<Candle> candles, int refreshSeconds) {
        StringBuilder out = new StringBuilder();
        String now = HHMM.format(Instant.now());
        if (candles.isEmpty()) {
            return out.append(BOLD).append(symbol).append(RESET)
                    .append("  no candles yet: are the engine and the simulator running?  (UTC ").append(now).append(")\n")
                    .toString();
        }

        BigDecimal high = candles.stream().map(Candle::high).reduce(BigDecimal::max).orElseThrow();
        BigDecimal low = candles.stream().map(Candle::low).reduce(BigDecimal::min).orElseThrow();
        BigDecimal range = high.subtract(low).max(new BigDecimal("0.01"));
        Candle last = candles.getLast();
        Candle first = candles.getFirst();
        BigDecimal change = last.close().subtract(first.open());
        String colour = change.signum() >= 0 ? GREEN : RED;

        out.append(BOLD).append(symbol).append(RESET).append("  1-minute candles   last ")
                .append(colour).append(BOLD).append(last.close()).append(RESET)
                .append(colour).append(change.signum() >= 0 ? "  +" : "  ").append(change).append(RESET)
                .append("   last candle: ").append(last.trades()).append(" trades, ").append(last.volumeLots()).append(" lots")
                .append(DIM).append("   UTC ").append(now).append(RESET).append("\n\n");

        // one text row per price band, from the highest price down
        for (int row = 0; row < CHART_ROWS; ++row) {
            BigDecimal rowTop = high.subtract(range.multiply(BigDecimal.valueOf(row)).divide(BigDecimal.valueOf(CHART_ROWS), 8, java.math.RoundingMode.HALF_UP));
            BigDecimal rowBottom = high.subtract(range.multiply(BigDecimal.valueOf(row + 1)).divide(BigDecimal.valueOf(CHART_ROWS), 8, java.math.RoundingMode.HALF_UP));
            out.append(DIM).append(String.format("%10s ┤", rowTop.setScale(2, java.math.RoundingMode.HALF_UP))).append(RESET);
            for (Candle candle : candles) {
                out.append(cell(candle, rowTop, rowBottom)).append(' ');
            }
            out.append('\n');
        }

        // time axis: first and last minute
        out.append(" ".repeat(12)).append(DIM).append(HHMM.format(Instant.ofEpochSecond(first.minute())));
        int gap = Math.max(1, candles.size() * 2 - 10);
        out.append(" ".repeat(gap)).append(HHMM.format(Instant.ofEpochSecond(last.minute()))).append(RESET).append('\n');
        out.append(DIM).append("refresh every ").append(refreshSeconds).append(" s · green: close ≥ open, red: close < open · Ctrl+C quits")
                .append(RESET).append('\n');
        return out.toString();
    }

    // a candle's character in one price band: body (open..close) is a block, the rest of the
    // high..low range is a thin line, outside the range is blank
    private static String cell(Candle candle, BigDecimal rowTop, BigDecimal rowBottom) {
        BigDecimal bodyTop = candle.open().max(candle.close());
        BigDecimal bodyBottom = candle.open().min(candle.close());
        boolean inRange = candle.high().compareTo(rowBottom) >= 0 && candle.low().compareTo(rowTop) <= 0;
        if (!inRange) {
            return " ";
        }
        String colour = candle.close().compareTo(candle.open()) >= 0 ? GREEN : RED;
        boolean inBody = bodyTop.compareTo(rowBottom) >= 0 && bodyBottom.compareTo(rowTop) <= 0;
        return colour + (inBody ? "█" : "│") + RESET;
    }
}
