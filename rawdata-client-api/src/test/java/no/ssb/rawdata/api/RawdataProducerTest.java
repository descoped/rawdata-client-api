package no.ssb.rawdata.api;

import de.huxhorn.sulky.ulid.ULID;
import org.testng.annotations.Test;

public class RawdataProducerTest {

    @Test
    public void nextMonotonicUlidDoesNotThrowExceptionWhenClockMovesForward() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() - 5000);
        ULID.Value value = RawdataProducer.nextMonotonicUlid(ulid, prev);
    }

    @Test
    public void nextMonotonicUlidDoesNotThrowExceptionWhenClockMovesBackwardBySmallAmount() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() + 1000);
        ULID.Value value = RawdataProducer.nextMonotonicUlid(ulid, prev);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void nextMonotonicUlidDoesNotThrowExceptionWhenClockMovesBackwardByLargeAmount() {
        ULID ulid = new ULID();
        ULID.Value prev = ulid.nextValue(System.currentTimeMillis() + 50000);
        ULID.Value value = RawdataProducer.nextMonotonicUlid(ulid, prev);
    }
}
