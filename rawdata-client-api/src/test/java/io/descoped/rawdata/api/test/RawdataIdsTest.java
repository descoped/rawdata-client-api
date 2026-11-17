package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataIds;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawdataIdsTest {

    @Test
    void generatedIdsAreUuidV7WithTheCurrentTime() {
        long before = System.currentTimeMillis();
        UUID id = RawdataIds.generate();
        long after = System.currentTimeMillis();
        assertEquals(7, id.version());
        assertEquals(2, id.variant());
        assertTrue(RawdataIds.isValid(id));
        long timestamp = RawdataIds.timestamp(id);
        assertTrue(before <= timestamp && timestamp <= after, timestamp + " not in [" + before + ", " + after + "]");
    }

    /**
     * The layout PostgreSQL 18's uuidv7() and uuid_extract_timestamp() use: the first 48 bits are the milliseconds.
     */
    @Test
    void layoutMatchesPostgres() {
        // 2026-11-17T12:00:00.123Z = 1794916800123 ms = 0x01A1_E960_D67B
        UUID id = RawdataIds.beginningOf(1794916800123L);
        assertEquals("01a1e960-d67b-7000-8000-000000000000", id.toString());
        assertEquals(1794916800123L, RawdataIds.timestamp(UUID.fromString("01a1e960-d67b-7abc-9def-0123456789ab")));
    }

    @Test
    void beginningOfSortsBeforeEveryIdOfTheMillisecond() {
        for (int i = 0; i < 1000; i++) {
            UUID id = RawdataIds.generate();
            UUID beginning = RawdataIds.beginningOf(RawdataIds.timestamp(id));
            assertTrue(RawdataIds.ORDER.compare(beginning, id) < 0);
            assertTrue(RawdataIds.ORDER.compare(RawdataIds.beginningOf(RawdataIds.timestamp(id) + 1), id) > 0);
        }
        assertEquals(RawdataIds.beginningOf(0), RawdataIds.beginningOfTime());
    }

    @Test
    void orderIsUnsignedLikePostgres() {
        // UUID.compareTo compares signed longs and gets these wrong; ORDER compares bytes, as PostgreSQL does
        UUID small = UUID.fromString("7fffffff-ffff-7fff-bfff-ffffffffffff");
        UUID large = UUID.fromString("80000000-0000-7000-8000-000000000000");
        assertTrue(RawdataIds.ORDER.compare(small, large) < 0);
        assertTrue(small.compareTo(large) > 0, "UUID.compareTo is signed, which is why ORDER exists");
    }

    @Test
    void nextIsAlwaysGreater() {
        UUID previous = RawdataIds.beginningOfTime();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            UUID next = RawdataIds.next(previous);
            assertTrue(RawdataIds.ORDER.compare(next, previous) > 0);
            assertTrue(RawdataIds.isValid(next));
            ids.add(next);
            previous = next;
        }
        assertEquals(ids.size(), ids.stream().distinct().count());
    }

    @Test
    void nextAfterAnIdInTheFutureKeepsItsMillisecond() {
        UUID future = UUID.fromString("ffffffff-0000-7000-8000-000000000000");
        UUID next = RawdataIds.next(future);
        assertEquals(RawdataIds.timestamp(future), RawdataIds.timestamp(next));
        assertEquals(UUID.fromString("ffffffff-0000-7000-8000-000000000001"), next);
    }

    @Test
    void nextCarriesIntoRandAAndTheTimestamp() {
        assertEquals(UUID.fromString("fffffff0-0000-7001-8000-000000000000"),
                RawdataIds.next(UUID.fromString("fffffff0-0000-7000-bfff-ffffffffffff")));
        assertEquals(UUID.fromString("fffffff0-0001-7000-8000-000000000000"),
                RawdataIds.next(UUID.fromString("fffffff0-0000-7fff-bfff-ffffffffffff")));
        UUID greatest = UUID.fromString("ffffffff-ffff-7fff-bfff-ffffffffffff");
        assertThrows(IllegalStateException.class, () -> RawdataIds.next(greatest));
    }

    @Test
    void otherUuidsAreRefused() {
        assertFalse(RawdataIds.isValid(UUID.randomUUID()));
        assertFalse(RawdataIds.isValid(null));
        UUID version4 = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> RawdataIds.timestamp(version4));
        assertThrows(IllegalArgumentException.class, () -> RawdataIds.next(version4));
        assertThrows(NullPointerException.class, () -> RawdataIds.requireValid(null));
    }

    @Test
    void timestampsOutsideTheRangeAreClamped() {
        assertEquals(RawdataIds.beginningOfTime(), RawdataIds.beginningOf(-5));
        assertEquals(0xFFFF_FFFF_FFFFL, RawdataIds.timestamp(RawdataIds.beginningOf(Long.MAX_VALUE)));
    }
}
