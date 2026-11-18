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
 * AIMD on concurrent uploads: start at 4, one more per success up to the cap, half on throttling, at most once a
 * second.
 */
@Timeout(10)
class AvroUploadLimiterTest {

    @Test
    void theLimitGrowsByOneAndHalvesOnThrottling() throws InterruptedException {
        AtomicLong now = new AtomicLong();
        AvroUploadLimiter limiter = new AvroUploadLimiter(16, now::get);
        assertEquals(4, limiter.limit());
        for (int i = 0; i < 20; i++) {
            limiter.acquire();
            limiter.release(true);
        }
        assertEquals(16, limiter.limit(), "capped");
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
        limiter.acquire();
        limiter.release(false);
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
        limiter.acquire();
        Thread second = new Thread(() -> {
            try {
                limiter.acquire();
                limiter.release(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        second.start();
        second.join(200);
        assertTrue(second.isAlive(), "waits for the running upload");
        limiter.release(true);
        second.join(5_000);
        assertFalse(second.isAlive());
    }
}
