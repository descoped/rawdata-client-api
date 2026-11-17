package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataTopics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawdataTopicsTest {

    @ParameterizedTest
    @ValueSource(strings = {"a", "Z", "0", "the-topic", "a.b_c-d", "9lives"})
    void validNames(String name) {
        assertTrue(RawdataTopics.isValid(name));
        assertEquals(name, RawdataTopics.requireValid(name));
    }

    @Test
    void hundredCharactersIsTheLimit() {
        assertTrue(RawdataTopics.isValid("a".repeat(100)));
        assertFalse(RawdataTopics.isValid("a".repeat(101)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "..", ".", "../x", "a/b", "a\\b", "-a", "_a", ".a", "a b", "a\nb", "a\"b", "a'b", "a;b", "a$b", "æøå", "a%2Fb"})
    void invalidNames(String name) {
        assertFalse(RawdataTopics.isValid(name));
        assertThrows(IllegalArgumentException.class, () -> RawdataTopics.requireValid(name));
    }

    @Test
    void nullIsRefused() {
        assertFalse(RawdataTopics.isValid(null));
        assertThrows(NullPointerException.class, () -> RawdataTopics.requireValid(null));
    }
}
