package io.descoped.rawdata.provider.avro;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AIMD on concurrent uploads: start at 4, one more after a round at the limit up to the cap, half on throttling, at
 * most once a second.
 */
@Timeout(10)
class AvroUploadLimiterTest {

    /**
     * Keeps the limit full for a number of uploads, all succeeding: fills the free places, then starts one upload each
     * time one ends, as uploads of many busy topics do.
     */
    private static void saturate(AvroUploadLimiter limiter, int uploads) throws InterruptedException {
        java.util.ArrayDeque<Boolean> runningAtLimit = new java.util.ArrayDeque<>();
        for (int started = 0; started < uploads; started++) {
            while (runningAtLimit.size() >= limiter.limit()) {
                limiter.release(true, runningAtLimit.pollFirst());
            }
            runningAtLimit.addLast(limiter.acquire());
        }
        while (!runningAtLimit.isEmpty()) {
            limiter.release(true, runningAtLimit.pollFirst());
        }
    }

    @Test
    void theLimitGrowsByOneARoundAtTheLimit() throws InterruptedException {
        AvroUploadLimiter limiter = new AvroUploadLimiter(16);
        assertEquals(4, limiter.limit());
        saturate(limiter, 4 + 4); // fills 4 places, then 4 more uploads each take the last place: one round
        assertEquals(5, limiter.limit(), "one more after a round of 4");
        saturate(limiter, 1_000);
        assertEquals(16, limiter.limit(), "capped");
    }

    @Test
    void aLimitThatIsNotReachedDoesNotGrow() throws InterruptedException {
        AvroUploadLimiter limiter = new AvroUploadLimiter(16);
        for (int i = 0; i < 100; i++) {
            boolean atLimit = limiter.acquire(); // one upload at a time: one busy topic
            assertFalse(atLimit);
            limiter.release(true, atLimit);
        }
        assertEquals(4, limiter.limit());
    }

    @Test
    void throttlingHalvesTheLimitOnceABurst() throws InterruptedException {
        AtomicLong now = new AtomicLong();
        AvroUploadLimiter limiter = new AvroUploadLimiter(16, now::get);
        saturate(limiter, 1_000);
        assertEquals(16, limiter.limit());
        limiter.throttled();
        assertEquals(8, limiter.limit());
        limiter.throttled();
        assertEquals(8, limiter.limit(), "one burst of throttling is one decrease");
        now.addAndGet(TimeUnit.SECONDS.toNanos(1));
        limiter.throttled();
        assertEquals(4, limiter.limit());
        for (int i = 0; i < 5; i++) {
            now.addAndGet(TimeUnit.SECONDS.toNanos(1));
            limiter.throttled();
        }
        assertEquals(1, limiter.limit(), "never below one");
        boolean atLimit = limiter.acquire();
        limiter.release(false, atLimit);
        assertEquals(1, limiter.limit(), "a failed upload does not raise it");
    }

    @Test
    void aCapBelowTheStartIsTheStart() {
        assertEquals(2, new AvroUploadLimiter(2).limit());
        assertThrows(IllegalArgumentException.class, () -> new AvroUploadLimiter(0));
    }

    @Test
    void anUploadWaitsWhileTheLimitIsReached() throws InterruptedException {
        AvroUploadLimiter limiter = new AvroUploadLimiter(1);
        boolean first = limiter.acquire();
        Thread second = new Thread(() -> {
            try {
                limiter.release(true, limiter.acquire());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        second.start();
        second.join(200);
        assertTrue(second.isAlive(), "waits for the running upload");
        limiter.release(true, first);
        second.join(5_000);
        assertFalse(second.isAlive());
    }
}
