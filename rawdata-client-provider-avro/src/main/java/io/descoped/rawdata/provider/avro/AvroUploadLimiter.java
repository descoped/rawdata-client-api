package io.descoped.rawdata.provider.avro;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * How many uploads of a client may run at once, across its topics: additive increase, multiplicative decrease (AIMD),
 * as TCP backs off. The limit starts at {@value #START} (or the cap, if lower), grows by one with each upload that
 * succeeds, up to {@code avro-file.upload.max-concurrency}, and halves when the store says it is throttled (GCS 429 or
 * 503, S3 503 SlowDown) or a request times out. A store reports a throttled response for each attempt of a request,
 * so the limit halves at most once a second: one burst of throttling is one decrease.
 * <p>
 * Within one topic uploads always run one at a time, in segment order; the limit is about many topics at once.
 */
final class AvroUploadLimiter {

    static final int START = 4;
    private static final long DECREASE_EVERY_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final int cap;
    private final LongSupplier nanoTime;
    private int limit; // guarded by this
    private int running; // guarded by this
    private long lastDecrease; // guarded by this
    private boolean decreased; // guarded by this

    AvroUploadLimiter(int cap) {
        this(cap, System::nanoTime);
    }

    AvroUploadLimiter(int cap, LongSupplier nanoTime) {
        if (cap < 1) {
            throw new IllegalArgumentException("The upload concurrency must be 1 or more: " + cap);
        }
        this.cap = cap;
        this.nanoTime = nanoTime;
        this.limit = Math.min(START, cap);
    }

    /**
     * Waits until another upload may start, and counts it as running.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    synchronized void acquire() throws InterruptedException {
        while (running >= limit) {
            wait();
        }
        running++;
    }

    /**
     * An upload ended.
     *
     * @param succeeded true if it succeeded, which raises the limit by one
     */
    synchronized void release(boolean succeeded) {
        running--;
        if (succeeded && limit < cap) {
            limit++;
        }
        notifyAll();
    }

    /**
     * The store was throttled or timed out: halves the limit, at most once a second.
     */
    synchronized void throttled() {
        long now = nanoTime.getAsLong();
        if (decreased && now - lastDecrease < DECREASE_EVERY_NANOS) {
            return;
        }
        decreased = true;
        lastDecrease = now;
        limit = Math.max(1, limit / 2);
    }

    synchronized int limit() {
        return limit;
    }
}
