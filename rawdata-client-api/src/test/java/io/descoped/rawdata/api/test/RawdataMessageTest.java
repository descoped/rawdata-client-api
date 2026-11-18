package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RawdataMessageTest {

    @Test
    void builderChangesAfterBuildDoNotReachTheMessage() {
        RawdataMessage.Builder builder = RawdataMessage.builder().position("p").put("k", new byte[]{1});
        RawdataMessage message = builder.build();
        builder.put("later", new byte[]{2}).put("k", new byte[]{9});
        assertEquals(List.of("k"), List.copyOf(message.keys()));
        assertArrayEquals(new byte[]{1}, message.get("k"));
    }

    @Test
    void contentIsCopiedInAndOut() {
        byte[] payload = {1, 2, 3};
        RawdataMessage message = RawdataMessage.builder().position("p").put("k", payload).build();
        payload[0] = 9;
        message.get("k")[1] = 9;
        message.data().get("k")[2] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, message.get("k"));
    }

    @Test
    void keysAreUnmodifiable() {
        RawdataMessage message = RawdataMessage.builder().position("p").put("k", new byte[0]).build();
        Set<String> keys = message.keys();
        Set<String> builderKeys = message.copy().keys();
        assertThrows(UnsupportedOperationException.class, () -> keys.remove("k"));
        assertThrows(UnsupportedOperationException.class, builderKeys::clear);
    }

    @Test
    void keysKeepTheirOrder() {
        RawdataMessage message = RawdataMessage.builder().position("p").put("z", new byte[0]).put("a", new byte[0]).put("m", new byte[0]).build();
        assertEquals(List.of("z", "a", "m"), List.copyOf(message.keys()));
        assertEquals(List.of("z", "a", "m"), List.copyOf(message.data().keySet()));
    }

    @Test
    void equalsAndHashCodeCompareContent() {
        UUID id = RawdataIds.generate();
        RawdataMessage a = RawdataMessage.builder().id(id).position("p").put("k", new byte[]{1, 2}).build();
        RawdataMessage b = RawdataMessage.builder().id(id).position("p").put("k", new byte[]{1, 2}).build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, RawdataMessage.builder().id(id).position("p").put("k", new byte[]{1, 3}).build());
        assertNotEquals(a, RawdataMessage.builder().id(id).orderingGroup("g").position("p").put("k", new byte[]{1, 2}).build());
        assertNotEquals(a, RawdataMessage.builder().position("p").put("k", new byte[]{1, 2}).build());
    }

    @Test
    void messageWithoutIdAndGroupCanBeCompared() {
        RawdataMessage a = RawdataMessage.builder().position("p").build();
        assertEquals(a, a.copy().build());
        assertNull(a.id());
        assertNull(a.orderingGroup());
    }

    @Test
    void copyKeepsEveryField() {
        UUID id = RawdataIds.generate();
        RawdataMessage message = RawdataMessage.builder().id(id).orderingGroup("g").sequenceNumber(3).position("p").put("k", new byte[]{7}).build();
        RawdataMessage copy = message.copy().build();
        assertEquals(message, copy);
        assertEquals(RawdataIds.timestamp(id), copy.timestamp());
        assertEquals(3, copy.sequenceNumber());
    }

    @Test
    void sequenceNumberMustNotBeNegative() {
        RawdataMessage.Builder builder = RawdataMessage.builder();
        assertThrows(IllegalArgumentException.class, () -> builder.sequenceNumber(-1));
        assertEquals(0, RawdataMessage.builder().sequenceNumber(0).sequenceNumber());
    }

    @Test
    void idMustBeAUuidV7() {
        RawdataMessage.Builder builder = RawdataMessage.builder();
        UUID version4 = UUID.randomUUID();
        UUID version6 = UUID.fromString("017f22e2-79b0-6cc4-98c0-0a5f4c1d2e3f");
        assertThrows(IllegalArgumentException.class, () -> builder.id(version4));
        assertThrows(IllegalArgumentException.class, () -> builder.id(version6));
        assertNull(builder.id(null).id());
    }

    @Test
    void positionIsRequired() {
        RawdataMessage.Builder builder = RawdataMessage.builder();
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void nullKeyOrPayloadIsRefused() {
        RawdataMessage.Builder builder = RawdataMessage.builder();
        byte[] empty = new byte[0];
        assertThrows(NullPointerException.class, () -> builder.put(null, empty));
        assertThrows(NullPointerException.class, () -> builder.put("k", null));
    }

    @Test
    void builderDataAddsAll() {
        RawdataMessage message = RawdataMessage.builder().position("p").data(Map.of("a", new byte[]{1})).build();
        assertArrayEquals(new byte[]{1}, message.get("a"));
        assertNull(message.get("missing"));
    }
}
