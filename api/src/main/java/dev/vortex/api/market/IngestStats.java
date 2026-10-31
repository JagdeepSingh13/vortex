package dev.vortex.api.market;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

/**
 * Counts and latency of the trade -> Redis path. Latency per trade = when its Redis update
 * finished (wall clock) - the engine's event_time_ns; both clocks are the same machine's.
 * Samples go into a fixed ring buffer, so memory stays flat however long the service runs.
 */
@Component
public class IngestStats {

    private static final int CAPACITY = 1 << 20;  // last ~1M trades

    private final long[] latencyMicros = new long[CAPACITY];
    private final AtomicLong next = new AtomicLong();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public void applied(long eventTimeNs) {
        applied.incrementAndGet();
        // Instant.now() has microsecond precision on Linux; currentTimeMillis() only milliseconds
        Instant now = Instant.now();
        long nowNs = now.getEpochSecond() * 1_000_000_000L + now.getNano();
        long micros = Math.max(0, (nowNs - eventTimeNs) / 1_000);
        latencyMicros[(int) (next.getAndIncrement() & (CAPACITY - 1))] = micros;
    }

    public void duplicate() { duplicates.incrementAndGet(); }

    public void rejected() { rejected.incrementAndGet(); }

    public void reset() {
        next.set(0);
        applied.set(0);
        duplicates.set(0);
        rejected.set(0);
    }

    public Snapshot snapshot() {
        int n = (int) Math.min(next.get(), CAPACITY);
        long[] sorted = Arrays.copyOf(latencyMicros, n);
        Arrays.sort(sorted);
        return new Snapshot(applied.get(), duplicates.get(), rejected.get(), n,
                at(sorted, 0.50), at(sorted, 0.90), at(sorted, 0.99), at(sorted, 0.999),
                n == 0 ? 0 : sorted[n - 1]);
    }

    private static long at(long[] sorted, double q) {
        return sorted.length == 0 ? 0 : sorted[(int) (q * (sorted.length - 1))];
    }

    public record Snapshot(long applied, long duplicates, long rejected, int samples,
                           long p50Micros, long p90Micros, long p99Micros, long p999Micros, long maxMicros) {
    }
}
