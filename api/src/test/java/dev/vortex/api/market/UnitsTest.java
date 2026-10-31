package dev.vortex.api.market;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Pure logic, no containers: which candle a trade belongs to, and exact tick -> price. */
class UnitsTest {

    private static final long MINUTE_NS = 60_000_000_000L;
    private final SymbolRegistry symbols = new SymbolRegistry("../config/symbols.yaml");

    private static TradeEvent at(long eventTimeNs) {
        return new TradeEvent("9f3c2a71b4e08d56", "AAPL", 1, 19005, 10, "BUY", eventTimeNs);
    }

    @Test
    void tradeBelongsToTheMinuteOfItsEngineEventTime() {
        long minute = 29_890_500L * MINUTE_NS;  // some minute in 2026, exactly on the boundary
        assertThat(at(minute).minuteEpochSeconds()).isEqualTo(minute / 1_000_000_000L);
        assertThat(at(minute + MINUTE_NS - 1).minuteEpochSeconds()).isEqualTo(minute / 1_000_000_000L);
        assertThat(at(minute + MINUTE_NS).minuteEpochSeconds()).isEqualTo(minute / 1_000_000_000L + 60);
    }

    @Test
    void ticksBecomeAnExactDecimalString() {
        assertThat(symbols.price("AAPL", 19005)).isEqualTo("190.05");
        assertThat(symbols.price("NFLX", 65000)).isEqualTo("650.00");
        assertThat(symbols.price("NVDA", 1)).isEqualTo("0.01");
        assertThat(symbols.isKnown("GOOG")).isFalse();
        assertThat(symbols.symbols()).containsExactly("AAPL", "MSFT", "GOOGL", "NFLX", "AMZN", "NVDA");
    }
}
