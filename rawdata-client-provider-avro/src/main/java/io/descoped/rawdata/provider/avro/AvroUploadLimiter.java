package io.descoped.rawdata.provider.avro;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * How many uploads of a client may run at once, across its topics: additive increase, multiplicative decrease (AIMD).
 * The limit starts at {@value #START} (or the cap, if lower) and:
 * <ul>
 *     <li>grows by one after a round at the limit: as many successful uploads as the limit, each of which took the last
 *     free place when it started, up to {@code avro-file.upload.max-concurrency}. While uploads keep the limit full,
 *     every upload takes the last place, so that is one more per round. A limit that uploads do not reach does not
 *     grow, so a burst across many topics starts where the limit was earned, and under steady throttling the limit
 *     settles instead of swinging;</li>
 *     <li>halves when an upload is throttled (GCS 429 or 503, S3 503 SlowDown) or times out. Only uploads count: a
 *     consumer's read or a busy metadata entry says nothing about the bucket's upload capacity. A store reports each
 *     attempt of a request, so the limit halves at most once a second: one burst of throttling is one decrease.</li>
 * </ul>
 * Within one topic uploads always run one at a time, in segment order; the limit is about many topics at once.
 */
final class AvroUploadLimiter {

    static final int START = 4;
    private static final long DECREASE_EVERY_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final int cap;
    private final LongSupplier nanoTime;
    private int limit; // guarded by this
    private int running; // guarded by this
    private int roundSuccesses; // guarded by this: successful uploads at the limit since the limit last changed
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
     * @return true if it took the last free place: it runs at the limit, and its success counts toward a round
     * @throws InterruptedException if interrupted while waiting
     */
    synchronized boolean acquire() throws InterruptedException {
        while (running >= limit) {
            wait();
        }
        running++;
        return running == limit;
    }

    /**
     * An upload ended.
     *
     * @param succeeded true if it succeeded
     * @param atLimit   what {@link #acquire()} returned for it
     */
    synchronized void release(boolean succeeded, boolean atLimit) {
        running--;
        if (succeeded && atLimit && limit < cap && ++roundSuccesses >= limit) {
            limit++;
            roundSuccesses = 0;
        }
        notifyAll();
    }

    /**
     * An upload was throttled or timed out: halves the limit, at most once a second.
     */
    synchronized void throttled() {
        long now = nanoTime.getAsLong();
        if (decreased && now - lastDecrease < DECREASE_EVERY_NANOS) {
            return;
        }
        decreased = true;
        lastDecrease = now;
        limit = Math.max(1, limit / 2);
        roundSuccesses = 0;
    }

    synchronized int limit() {
        return limit;
    }
}
