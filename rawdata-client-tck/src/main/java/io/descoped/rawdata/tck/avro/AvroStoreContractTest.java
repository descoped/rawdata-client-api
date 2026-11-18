package io.descoped.rawdata.tck.avro;

import io.descoped.rawdata.provider.avro.AvroStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The contract of an {@link AvroStore}, which the Avro engine relies on, as JUnit tests. A provider built on the
 * engine extends this class with its store; together with
 * {@link io.descoped.rawdata.tck.RawdataClientContractTest} it shows the provider behaves as the others do. Every
 * test gets a new store and its own topic.
 */
@Timeout(60)
public abstract class AvroStoreContractTest {

    /**
     * The store under test, created before each test and closed after it.
     */
    protected AvroStore store;

    /**
     * The topic of the current test, unique per test.
     */
    protected String topic;

    /**
     * Creates the test; subclasses are instantiated by JUnit.
     */
    protected AvroStoreContractTest() {
    }

    /**
     * Creates a store on the provider's test location; stores created in one test share it.
     *
     * @return a new store
     * @throws Exception if the store cannot be created
     */
    protected abstract AvroStore createStore() throws Exception;

    /**
     * Creates a store whose location does not exist, for example a missing bucket; null if the store has none.
     *
     * @return a new store, or null
     * @throws Exception if the store cannot be created
     */
    protected AvroStore createMissingStore() throws Exception {
        return null;
    }

    /**
     * Creates the store and the topic.
     *
     * @throws Exception if the store cannot be created
     */
    @BeforeEach
    protected void createTheStore() throws Exception {
        store = createStore();
        topic = "store-" + UUID.randomUUID();
    }

    /**
     * Closes the store.
     *
     * @throws Exception if closing fails
     */
    @AfterEach
    protected void closeTheStore() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    /**
     * A segment is created only if it does not exist. Creating it again with the same bytes, as a retry after a lost
     * response does, counts as created; other bytes, another writer's, do not, and the segment keeps the first.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void aSegmentIsCreatedOnlyOnce() throws IOException {
        byte[] mine = {1, 2, 3};
        assertTrue(store.create(topic, 0, mine));
        assertTrue(store.create(topic, 0, mine.clone()), "the same bytes are this writer's create");
        assertFalse(store.create(topic, 0, new byte[]{9}), "other bytes are another writer's");
        assertArrayEquals(mine, store.read(topic, 0).orElseThrow());
        assertTrue(store.create(topic, 1, new byte[]{4}));
        assertTrue(store.exists(topic, 0));
        assertTrue(store.exists(topic, 1));
        assertFalse(store.exists(topic, 2));
        assertTrue(store.read(topic, 2).isEmpty());
    }

    /**
     * A segment's bytes come back as written, large ones included.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void segmentsComeBackAsWritten() throws IOException {
        byte[] large = new byte[3 * 1024 * 1024 + 7];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i * 31);
        }
        assertTrue(store.create(topic, 0, large));
        assertArrayEquals(large, store.read(topic, 0).orElseThrow());
    }

    /**
     * A segment is read in ranges from an opened segment: its size, any range, a range past its end cut short, and
     * nothing at its end.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void aSegmentIsReadInRanges() throws IOException {
        byte[] content = new byte[100_000];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 31);
        }
        assertTrue(store.create(topic, 0, content));
        AvroStore.Segment segment = store.open(topic, 0).orElseThrow();
        assertEquals(content.length, segment.size());
        assertArrayEquals(Arrays.copyOfRange(content, 0, 10), segment.read(0, 10));
        assertArrayEquals(Arrays.copyOfRange(content, 4_096, 70_000), segment.read(4_096, 70_000 - 4_096));
        assertArrayEquals(Arrays.copyOfRange(content, 99_990, 100_000), segment.read(99_990, 100), "cut short at the end");
        assertEquals(0, segment.read(100_000, 10).length, "nothing at the end");
        assertTrue(store.open(topic, 1).isEmpty());
    }

    /**
     * An unknown topic has no segments and no metadata.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void anUnknownTopicIsEmpty() throws IOException {
        assertFalse(store.exists(topic, 0));
        assertTrue(store.read(topic, 0).isEmpty());
        assertEquals(Set.of(), store.metadataNames(topic));
        assertTrue(store.readMetadata(topic, "k01").isEmpty());
    }

    /**
     * Metadata entries are replaced by the last write, listed and removed; removing twice is fine.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void metadataIsReplacedListedAndRemoved() throws IOException {
        store.writeMetadata(topic, "k01", new byte[]{1});
        store.writeMetadata(topic, "k01", new byte[]{2});
        store.writeMetadata(topic, "k02", new byte[]{3});
        assertArrayEquals(new byte[]{2}, store.readMetadata(topic, "k01").orElseThrow());
        assertEquals(Set.of("k01", "k02"), store.metadataNames(topic));
        store.deleteMetadata(topic, "k01");
        store.deleteMetadata(topic, "k01");
        assertTrue(store.readMetadata(topic, "k01").isEmpty());
        assertEquals(Set.of("k02"), store.metadataNames(topic));
    }

    /**
     * Topics are apart, including topics whose names differ only by case, and segments and metadata do not mix.
     *
     * @throws IOException if the store fails
     */
    @Test
    public void topicsAreApart() throws IOException {
        String upper = topic + "-Case";
        String lower = topic + "-case";
        assertTrue(store.create(upper, 0, new byte[]{1}));
        assertTrue(store.create(lower, 0, new byte[]{2}));
        store.writeMetadata(upper, "k01", new byte[]{3});
        assertArrayEquals(new byte[]{1}, store.read(upper, 0).orElseThrow());
        assertArrayEquals(new byte[]{2}, store.read(lower, 0).orElseThrow());
        assertEquals(Set.of("k01"), store.metadataNames(upper));
        assertEquals(Set.of(), store.metadataNames(lower));
        assertFalse(store.exists(topic, 0));
    }

    /**
     * A store whose location does not exist fails with IOException; it never reports an empty topic.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    public void aMissingLocationFails() throws Exception {
        AvroStore missing = createMissingStore();
        assumeTrue(missing != null, "the store has no location that can be missing");
        try (missing) {
            assertThrows(IOException.class, () -> missing.exists(topic, 0));
            assertThrows(IOException.class, () -> missing.read(topic, 0));
            assertThrows(IOException.class, () -> missing.open(topic, 0));
            assertThrows(IOException.class, () -> missing.create(topic, 0, new byte[]{1}));
            assertThrows(IOException.class, () -> missing.metadataNames(topic));
            assertThrows(IOException.class, () -> missing.readMetadata(topic, "k01"));
            assertThrows(IOException.class, () -> missing.writeMetadata(topic, "k01", new byte[]{1}));
        }
    }
}
