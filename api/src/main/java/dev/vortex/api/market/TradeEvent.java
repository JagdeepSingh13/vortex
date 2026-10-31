package dev.vortex.api.market;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The fields of a trade.v1 message this service needs. Prices and quantities stay integers
 * (ticks and lots); decimals appear only when presenting them.
 */
public record TradeEvent(
        @JsonProperty("engine_run_id") String engineRunId,
        @JsonProperty("symbol") String symbol,
        @JsonProperty("match_id") long matchId,
        @JsonProperty("price_ticks") long priceTicks,
        @JsonProperty("qty_lots") long qtyLots,
        @JsonProperty("aggressor_side") String aggressorSide,
        @JsonProperty("event_time_ns") long eventTimeNs) {

    private static final long NANOS_PER_MINUTE = 60_000_000_000L;

    /** Start of the trade's 1-minute candle in epoch seconds, from the engine's event time. */
    public long minuteEpochSeconds() {
        return Math.floorDiv(eventTimeNs, NANOS_PER_MINUTE) * 60;
    }
}
