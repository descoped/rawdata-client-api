package io.descoped.rawdata.tck;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCopy;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataWriterConflictException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The contract every {@link RawdataClient} provider keeps, as JUnit tests. A provider's test extends this class and
 * says how to configure a client; every test gets a fresh client and its own topic, so tests do not see each other's
 * messages even when the provider keeps data between clients.
 * <p>
 * A provider is compliant when it passes these tests unchanged: an application can then swap it for another. A
 * provider's own tests only add what the API cannot see (its SQL, its files or objects, its configuration). Only one
 * process writes a topic; tests with a second client on the same store play another process.
 */
@Timeout(60)
public abstract class RawdataClientContractTest {

    /**
     * The client under test, created before each test and closed after it.
     */
    protected RawdataClient client;

    /**
     * The topic of the current test, unique per test.
     */
    protected String topic;

    /**
     * Creates the test; subclasses are instantiated by JUnit.
     */
    protected RawdataClientContractTest() {
    }

    /**
     * The configuration of the provider under test, including {@link RawdataClient#PROVIDER_KEY}.
     *
     * @return the configuration
     */
    protected abstract Map<String, String> configuration();

    /**
     * Whether two clients made from {@link #configuration()} share a store, as two processes on one database, folder
     * or bucket do. True for every provider that keeps data outside the client; the tests with a second client are
     * skipped where it is false.
     *
     * @return true if clients share the store
     */
    protected boolean clientsShareTheStore() {
        return true;
    }

    /**
     * The longest metadata key the provider takes for a topic, in UTF-8 bytes, as its documentation states; empty if
     * it has no limit. Every provider takes at least {@link RawdataMetadataClient#PORTABLE_KEY_BYTES}.
     *
     * @param topic the topic, for stores where the limit depends on it
     * @return the limit, or empty
     */
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.empty();
    }

    /**
     * A configuration of the provider whose store does not exist, for example a missing bucket, database or folder;
     * null if the provider has no such thing.
     *
     * @return the configuration, or null
     */
    protected Map<String, String> missingStoreConfiguration() {
        return null;
    }

    /**
     * Creates the client and a fresh topic name.
     */
    @BeforeEach
    protected void createClient() {
        client = RawdataClient.create(configuration());
        topic = "tck-" + UUID.randomUUID();
    }

    /**
     * Closes the client.
     */
    @AfterEach
    protected void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    private static RawdataMessage message(String position, int... sizes) {
        RawdataMessage.Builder builder = RawdataMessage.builder().position(position);
        for (int i = 0; i < sizes.length; i++) {
            byte[] payload = new byte[sizes[i]];
            for (int b = 0; b < payload.length; b++) {
                payload[b] = (byte) (position.hashCode() + i + b);
            }
            builder.put("payload" + (i + 1), payload);
        }
        return builder.build();
    }

    private void publish(String... positions) {
        try (RawdataProducer producer = client.producer(topic)) {
            for (String position : positions) {
                producer.publish(message(position, 5, 3));
            }
        }
    }

    private List<String> readAll(RawdataConsumer consumer) throws InterruptedException {
        List<String> positions = new ArrayList<>();
        RawdataMessage message;
        while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
            positions.add(message.position());
        }
        return positions;
    }

    // ---- reading and writing

    /**
     * An unknown topic is empty.
     */
    @Test
    public void lastMessageOfEmptyTopicIsNull() {
        assertNull(client.lastMessage(topic));
    }

    /**
     * The last message follows what is published.
     */
    @Test
    public void lastMessageFollowsThePublishedMessages() {
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(message("a", 5), message("b", 3));
            assertEquals("b", client.lastMessage(topic).position());
            producer.publish(message("c", 7));
            assertEquals("c", client.lastMessage(topic).position());
        }
    }

    /**
     * Every field of a message survives the store.
     *
     * @throws Exception on failure
     */
    @Test
    public void allFieldsSurvive() throws Exception {
        UUID id = RawdataIds.generate();
        RawdataMessage written = RawdataMessage.builder().id(id).orderingGroup("og1").sequenceNumber(7).position("a")
                .put("payload1", new byte[]{1, 2, 3}).put("payload2", new byte[]{4, 5}).build();
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(written);
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            RawdataMessage read = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(read);
            assertEquals(id, read.id());
            assertEquals("og1", read.orderingGroup());
            assertEquals(7, read.sequenceNumber());
            assertEquals("a", read.position());
            assertEquals(List.of("payload1", "payload2"), List.copyOf(read.keys()));
            assertArrayEquals(new byte[]{1, 2, 3}, read.get("payload1"));
            assertArrayEquals(new byte[]{4, 5}, read.get("payload2"));
            assertEquals(written, read);
        }
    }

    /**
     * A message without content and without an ordering group round-trips.
     *
     * @throws Exception on failure
     */
    @Test
    public void messageWithoutContentSurvives() throws Exception {
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("empty").build());
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            RawdataMessage read = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(read);
            assertEquals("empty", read.position());
            assertNull(read.orderingGroup());
            assertEquals(Set.of(), read.keys());
        }
    }

    /**
     * An empty payload and a large payload round-trip.
     *
     * @throws Exception on failure
     */
    @Test
    public void emptyAndLargePayloadsSurvive() throws Exception {
        byte[] large = new byte[1024 * 1024 + 7];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i * 31);
        }
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("p").put("empty", new byte[0]).put("large", large).build());
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            RawdataMessage read = consumer.receive(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[0], read.get("empty"));
            assertArrayEquals(large, read.get("large"));
        }
    }

    /**
     * Changing an array after publishing it, or an array read back, changes nothing in the store.
     *
     * @throws Exception on failure
     */
    @Test
    public void contentIsCopiedInAndOut() throws Exception {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("a").put("k", payload).build());
        }
        payload[0] = 'J';
        RawdataMessage last = client.lastMessage(topic);
        last.get("k")[0] = 'Y';
        assertEquals("hello", new String(client.lastMessage(topic).get("k"), StandardCharsets.UTF_8));
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals("hello", new String(consumer.receive(5, TimeUnit.SECONDS).get("k"), StandardCharsets.UTF_8));
        }
    }

    /**
     * A message published after the consumer started waiting is received.
     *
     * @throws Exception on failure
     */
    @Test
    public void receiveWaitsForAMessage() throws Exception {
        try (RawdataConsumer consumer = client.consumer(topic)) {
            CompletableFuture.runAsync(() -> {
                sleep(300);
                publish("late");
            });
            RawdataMessage message = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals("late", message.position());
        }
    }

    /**
     * Receive returns null when nothing arrives before the timeout.
     *
     * @throws Exception on failure
     */
    @Test
    public void receiveReturnsNullOnTimeout() throws Exception {
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Messages of one producer are read in the order they were published.
     *
     * @throws Exception on failure
     */
    @Test
    public void messagesAreReadInOrder() throws Exception {
        try (RawdataProducer producer = client.producer(topic)) {
            producer.publish(message("a", 5), message("b", 3), message("c", 7));
            producer.publish(List.of(message("d", 1), message("e", 2)));
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(List.of("a", "b", "c", "d", "e"), readAll(consumer));
        }
    }

    /**
     * Producers that follow each other on a topic keep the order, also within one millisecond.
     *
     * @throws Exception on failure
     */
    @Test
    public void producersThatFollowEachOtherKeepTheOrder() throws Exception {
        for (int round = 0; round < 20; round++) {
            publish("r" + round + "a", "r" + round + "b");
        }
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < 20; round++) {
            expected.add("r" + round + "a");
            expected.add("r" + round + "b");
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(expected, readAll(consumer));
        }
    }

    /**
     * A generated id is greater than an id the caller gave the message before it.
     *
     * @throws Exception on failure
     */
    @Test
    public void generatedIdFollowsAGivenId() throws Exception {
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < 20; round++) {
            RawdataMessage last = client.lastMessage(topic);
            // a given id just after the topic's last, often in the same millisecond as the generated one that follows
            UUID given = RawdataIds.next(last == null ? RawdataIds.beginningOfTime() : last.id());
            try (RawdataProducer producer = client.producer(topic)) {
                producer.publish(
                        RawdataMessage.builder().id(given).position("r" + round + "given").build(),
                        RawdataMessage.builder().position("r" + round + "generated").build());
            }
            expected.add("r" + round + "given");
            expected.add("r" + round + "generated");
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(expected, readAll(consumer));
        }
    }

    /**
     * A given id that is not greater than every id before it is refused, and nothing of the call is written.
     */
    @Test
    public void givenIdNotGreaterThanTheTopicsIsRefused() {
        publish("a");
        RawdataMessage last = client.lastMessage(topic);
        UUID lower = RawdataIds.beginningOf(last.timestamp());
        RawdataMessage equal = RawdataMessage.builder().id(last.id()).position("equal").build();
        RawdataMessage before = RawdataMessage.builder().id(lower).position("lower").build();
        RawdataMessage fresh = RawdataMessage.builder().position("fresh").build();
        UUID ahead = RawdataIds.next(last.id());
        RawdataMessage first = RawdataMessage.builder().id(RawdataIds.next(ahead)).position("first").build();
        RawdataMessage second = RawdataMessage.builder().id(ahead).position("second").build();
        try (RawdataProducer producer = client.producer(topic)) {
            assertThrows(IllegalArgumentException.class, () -> producer.publish(equal));
            assertThrows(IllegalArgumentException.class, () -> producer.publish(fresh, before));
            assertThrows(IllegalArgumentException.class, () -> producer.publish(first, second), "ids must increase within a call too");
        }
        assertEquals("a", client.lastMessage(topic).position());
        assertEquals(last, client.lastMessage(topic));
    }

    /**
     * Producers in parallel threads lose nothing, and every message has a unique id.
     *
     * @throws Exception on failure
     */
    @Test
    public void parallelProducersLoseNothing() throws Exception {
        int threads = 4;
        int perThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch start = new CountDownLatch(1);
            for (int t = 0; t < threads; t++) {
                int thread = t;
                futures.add(executor.submit(() -> {
                    start.await();
                    try (RawdataProducer producer = client.producer(topic)) {
                        for (int i = 0; i < perThread; i++) {
                            producer.publish(message("t" + thread + "-" + i, 3));
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        List<RawdataMessage> read = new ArrayList<>();
        try (RawdataConsumer consumer = client.consumer(topic)) {
            RawdataMessage message;
            while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                read.add(message);
            }
        }
        assertEquals(threads * perThread, read.size());
        for (int i = 1; i < read.size(); i++) {
            assertTrue(RawdataIds.ORDER.compare(read.get(i - 1).id(), read.get(i).id()) < 0, "ids must strictly increase");
        }
        for (int t = 0; t < threads; t++) {
            String prefix = "t" + t + "-";
            List<String> ofThread = read.stream().map(RawdataMessage::position).filter(p -> p.startsWith(prefix)).toList();
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < perThread; i++) {
                expected.add(prefix + i);
            }
            assertEquals(expected, ofThread, "each producer's own order is kept");
        }
    }

    /**
     * Asynchronous publish and receive.
     *
     * @throws Exception on failure
     */
    @Test
    public void asynchronousPublishAndReceive() throws Exception {
        try (RawdataProducer producer = client.producer(topic); RawdataConsumer consumer = client.consumer(topic)) {
            CompletableFuture<? extends RawdataMessage> received = consumer.receiveAsync();
            producer.publishAsync(message("a", 5)).get(30, TimeUnit.SECONDS);
            assertEquals("a", received.get(30, TimeUnit.SECONDS).position());
            producer.publishAsync(List.of(message("b", 1), message("c", 1))).get(30, TimeUnit.SECONDS);
            assertEquals("b", consumer.receiveAsync().get(30, TimeUnit.SECONDS).position());
            assertEquals("c", consumer.receiveAsync().get(30, TimeUnit.SECONDS).position());
        }
    }

    /**
     * Two consumers read the same messages independently.
     *
     * @throws Exception on failure
     */
    @Test
    public void consumersReadIndependently() throws Exception {
        publish("a", "b", "c");
        try (RawdataConsumer first = client.consumer(topic); RawdataConsumer second = client.consumer(topic)) {
            assertEquals("a", first.receive(5, TimeUnit.SECONDS).position());
            assertEquals(List.of("a", "b", "c"), readAll(second));
            assertEquals(List.of("b", "c"), readAll(first));
        }
    }

    // ---- starting points

    /**
     * A consumer can start after or at a position.
     *
     * @throws Exception on failure
     */
    @Test
    public void consumerStartsAtAPosition() throws Exception {
        publish("a", "b", "c", "d");
        long now = System.currentTimeMillis();
        try (RawdataConsumer consumer = client.consumer(topic, "a", now, Duration.ofMinutes(1))) {
            assertEquals("b", consumer.receive(5, TimeUnit.SECONDS).position());
        }
        try (RawdataConsumer consumer = client.consumer(topic, "b", now, Duration.ofMinutes(1))) {
            assertEquals("c", consumer.receive(5, TimeUnit.SECONDS).position());
        }
        try (RawdataConsumer consumer = client.consumer(topic, "c", true, now, Duration.ofMinutes(1))) {
            assertEquals("c", consumer.receive(5, TimeUnit.SECONDS).position());
        }
        try (RawdataConsumer consumer = client.consumer(topic, "d", now, Duration.ofMinutes(1))) {
            assertNull(consumer.receive(200, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * A consumer can start after or at an id.
     *
     * @throws Exception on failure
     */
    @Test
    public void consumerStartsAtAnId() throws Exception {
        publish("a", "b", "c");
        UUID idOfB;
        try (RawdataConsumer consumer = client.consumer(topic)) {
            consumer.receive(5, TimeUnit.SECONDS);
            idOfB = consumer.receive(5, TimeUnit.SECONDS).id();
        }
        try (RawdataConsumer consumer = client.consumer(topic, idOfB)) {
            assertEquals(List.of("c"), readAll(consumer));
        }
        try (RawdataConsumer consumer = client.consumer(topic, idOfB, true)) {
            assertEquals(List.of("b", "c"), readAll(consumer));
        }
    }

    /**
     * The search range of a position includes both ends, so a tolerance of 0 finds a message written in exactly that
     * millisecond.
     */
    @Test
    public void positionIsFoundWithZeroTolerance() {
        publish("a");
        long timestamp = client.lastMessage(topic).timestamp();
        assertNotNull(client.cursorOf(topic, "a", true, timestamp, Duration.ZERO));
    }

    /**
     * An unknown position, or any position in an empty topic, is not found.
     */
    @Test
    public void unknownPositionIsNotFound() {
        assertThrows(RawdataNoSuchPositionException.class,
                () -> client.cursorOf(topic, "x", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        publish("a", "b");
        assertThrows(RawdataNoSuchPositionException.class,
                () -> client.cursorOf(topic, "x", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    /**
     * Seek moves to the first message written at or after a time.
     *
     * @throws Exception on failure
     */
    @Test
    public void seekMovesToATime() throws Exception {
        long[] before = new long[4];
        String[] positions = {"a", "b", "c", "d"};
        try (RawdataProducer producer = client.producer(topic)) {
            for (int i = 0; i < positions.length; i++) {
                sleep(5);
                before[i] = System.currentTimeMillis();
                sleep(5);
                producer.publish(message(positions[i], 3));
            }
        }
        sleep(5);
        long after = System.currentTimeMillis();
        try (RawdataConsumer consumer = client.consumer(topic)) {
            consumer.seek(after);
            assertNull(consumer.receive(200, TimeUnit.MILLISECONDS));
            consumer.seek(before[3]);
            assertEquals("d", consumer.receive(5, TimeUnit.SECONDS).position());
            consumer.seek(before[1]);
            assertEquals("b", consumer.receive(5, TimeUnit.SECONDS).position());
            consumer.seek(before[2]);
            assertEquals("c", consumer.receive(5, TimeUnit.SECONDS).position());
            consumer.seek(before[0]);
            assertEquals("a", consumer.receive(5, TimeUnit.SECONDS).position());
        }
    }

    // ---- metadata

    /**
     * Metadata entries can be written, listed, read, replaced and removed, and every metadata client of the topic sees
     * them.
     */
    @Test
    public void metadataIsKeptPerTopic() {
        RawdataMetadataClient metadata = client.metadata(topic);
        assertEquals(topic, metadata.topic());
        assertEquals(Set.of(), metadata.keys());
        assertNull(metadata.get("missing"));
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put("key-2", "Value-2".getBytes(StandardCharsets.UTF_8));
        RawdataMetadataClient again = client.metadata(topic);
        assertEquals(Set.of("key-1", "key-2"), again.keys());
        assertEquals("Value-1", new String(again.get("key-1"), StandardCharsets.UTF_8));
        again.put("key-2", "Overwritten".getBytes(StandardCharsets.UTF_8));
        assertEquals("Overwritten", new String(metadata.get("key-2"), StandardCharsets.UTF_8));
        metadata.remove("key-1").remove("key-1");
        assertEquals(Set.of("key-2"), metadata.keys());
        assertEquals(Set.of(), client.metadata(topic + "-other").keys());
    }

    /**
     * Metadata values are copied in and out.
     */
    @Test
    public void metadataIsCopiedInAndOut() {
        byte[] value = {1, 2, 3};
        RawdataMetadataClient metadata = client.metadata(topic);
        metadata.put("k", value);
        value[0] = 9;
        metadata.get("k")[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, metadata.get("k"));
    }

    /**
     * A metadata key is any string without NUL, up to the portable length: case, the empty string, dots and slashes
     * mean nothing special. A key with NUL is refused by every method that takes a key.
     */
    @Test
    public void metadataKeysAreAnyStringWithoutNul() {
        List<String> keys = List.of("Key", "key", "", "..", "../escape", "a/b", "ü € 😀", ".hidden",
                "x".repeat(RawdataMetadataClient.PORTABLE_KEY_BYTES));
        RawdataMetadataClient metadata = client.metadata(topic);
        for (int i = 0; i < keys.size(); i++) {
            metadata.put(keys.get(i), new byte[]{(byte) i});
        }
        assertEquals(Set.copyOf(keys), metadata.keys());
        for (int i = 0; i < keys.size(); i++) {
            assertArrayEquals(new byte[]{(byte) i}, metadata.get(keys.get(i)), keys.get(i));
        }
        metadata.remove("..");
        assertNull(metadata.get(".."));
        assertNotNull(metadata.get("../escape"));
        byte[] value = {1};
        assertThrows(IllegalArgumentException.class, () -> metadata.put("a\0b", value));
        assertThrows(IllegalArgumentException.class, () -> metadata.get("a\0b"));
        assertThrows(IllegalArgumentException.class, () -> metadata.remove("a\0b"));
        assertEquals(keys.size() - 1, metadata.keys().size());
    }

    /**
     * A key at the provider's limit works; one byte longer is refused with IllegalArgumentException by put, get and
     * remove, and nothing is written. A provider without a limit takes a long key.
     */
    @Test
    public void aMetadataKeyOverTheProvidersLimitIsRefused() {
        RawdataMetadataClient metadata = client.metadata(topic);
        OptionalInt limit = metadataKeyLimit(topic);
        assertTrue(limit.orElse(Integer.MAX_VALUE) >= RawdataMetadataClient.PORTABLE_KEY_BYTES, "below the portable length: " + limit);
        if (limit.isEmpty()) {
            String longKey = "k".repeat(10_000);
            metadata.put(longKey, new byte[]{1});
            assertArrayEquals(new byte[]{1}, metadata.get(longKey));
            return;
        }
        String atLimit = "k".repeat(limit.getAsInt());
        String overLimit = atLimit + "k";
        byte[] value = {1};
        metadata.put(atLimit, value);
        assertArrayEquals(value, metadata.get(atLimit));
        assertThrows(IllegalArgumentException.class, () -> metadata.put(overLimit, value));
        assertThrows(IllegalArgumentException.class, () -> metadata.get(overLimit));
        assertThrows(IllegalArgumentException.class, () -> metadata.remove(overLimit));
        assertEquals(Set.of(atLimit), metadata.keys());
    }

    // ---- one writer, several clients

    /**
     * Only one process writes a topic. A second client (another process) that publishes starts from the topic's end;
     * the first client's next publish then finds the topic moved and fails with RawdataWriterConflictException,
     * writing nothing, and keeps failing. The second writes on, and readers see each message once, in order.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void aSecondWriterMakesTheFirstFailAndTheTopicStaysWhole() throws InterruptedException {
        assumeTrue(clientsShareTheStore(), "the provider keeps a store per client");
        try (RawdataClient other = RawdataClient.create(configuration())) {
            client.producer(topic).publish(message("a"));
            other.producer(topic).publish(message("b"));
            RawdataProducer first = client.producer(topic);
            RawdataMessage c = message("c");
            RawdataWriterConflictException conflict = assertThrows(RawdataWriterConflictException.class, () -> first.publish(c));
            assertEquals(topic, conflict.topic());
            assertThrows(RawdataWriterConflictException.class, () -> client.producer(topic).publish(c), "the client keeps failing");
            other.producer(topic).publish(message("d"));
            try (RawdataConsumer consumer = other.consumer(topic)) {
                assertEquals(List.of("a", "b", "d"), readAll(consumer));
            }
            assertEquals("d", client.lastMessage(topic).position());
        }
    }

    /**
     * A writer that starts after another stopped (a restart) continues after the last message.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void aNewWriterContinuesAfterTheLastMessage() throws InterruptedException {
        assumeTrue(clientsShareTheStore(), "the provider keeps a store per client");
        publish("a", "b");
        client.close();
        client = RawdataClient.create(configuration());
        publish("c");
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(List.of("a", "b", "c"), readAll(consumer));
        }
    }

    /**
     * A consumer in another client sees what the writer publishes, by waiting.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    public void aConsumerOfAnotherClientSeesWhatIsPublished() throws Exception {
        assumeTrue(clientsShareTheStore(), "the provider keeps a store per client");
        try (RawdataClient reader = RawdataClient.create(configuration()); RawdataConsumer consumer = reader.consumer(topic)) {
            CompletableFuture<? extends RawdataMessage> received = consumer.receiveAsync();
            client.producer(topic).publish(message("a"));
            assertEquals("a", received.get(30, TimeUnit.SECONDS).position());
            assertEquals("a", reader.lastMessage(topic).position());
        }
    }

    /**
     * A publish whose batch is refused (a given id not after the topic's last) writes nothing of the batch, and the
     * writer goes on.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void aRefusedBatchWritesNothing() throws InterruptedException {
        publish("a");
        UUID last = client.lastMessage(topic).id();
        try (RawdataProducer producer = client.producer(topic)) {
            RawdataMessage fine = message("b", 3);
            RawdataMessage stale = RawdataMessage.builder().id(last).position("stale").build();
            assertThrows(IllegalArgumentException.class, () -> producer.publish(fine, stale));
            assertEquals("a", client.lastMessage(topic).position());
            producer.publish(message("c"));
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(List.of("a", "c"), readAll(consumer));
        }
    }

    /**
     * Many small publishes are all read, in order.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void manySmallPublishesAreAllRead() throws InterruptedException {
        try (RawdataProducer producer = client.producer(topic)) {
            for (int i = 0; i < 200; i++) {
                producer.publish(message("m" + i));
            }
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            List<String> read = new ArrayList<>();
            List<? extends RawdataMessage> batch;
            while (!(batch = consumer.receiveUpTo(500, Duration.ofSeconds(1))).isEmpty()) {
                batch.forEach(m -> read.add(m.position()));
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                expected.add("m" + i);
            }
            assertEquals(expected, read);
        }
    }

    /**
     * Seek, a start at a position and a start at an id find their message among many publishes.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void seekAndStartsFindTheirMessageAmongManyPublishes() throws InterruptedException {
        long base = System.currentTimeMillis() - 1_000_000;
        try (RawdataProducer producer = client.producer(topic)) {
            for (int i = 0; i < 100; i++) {
                producer.publish(RawdataMessage.builder().id(RawdataIds.beginningOf(base + 10L * i)).position("m" + i).build());
            }
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            consumer.seek(base + 505);
            assertEquals("m51", consumer.receive(5, TimeUnit.SECONDS).position());
            consumer.seek(base + 990);
            assertEquals("m99", consumer.receive(5, TimeUnit.SECONDS).position());
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(base);
            assertEquals("m0", consumer.receive(5, TimeUnit.SECONDS).position());
        }
        try (RawdataConsumer consumer = client.consumer(topic, "m70", base + 700, Duration.ZERO)) {
            assertEquals("m71", consumer.receive(5, TimeUnit.SECONDS).position());
        }
        try (RawdataConsumer consumer = client.consumer(topic, RawdataIds.beginningOf(base + 300), true)) {
            assertEquals("m30", consumer.receive(5, TimeUnit.SECONDS).position());
        }
    }

    // ---- a store that does not exist

    /**
     * A client on a store that does not exist fails, at creation or on use, with a RawdataException; it never reports
     * an empty topic.
     */
    @Test
    public void aStoreThatDoesNotExistFails() {
        Map<String, String> missing = missingStoreConfiguration();
        assumeTrue(missing != null, "the provider has no store that can be missing");
        RawdataClient broken;
        try {
            broken = RawdataClient.create(missing);
        } catch (RawdataException e) {
            return; // failing at creation is fine
        }
        try (broken) {
            assertThrows(RawdataException.class, () -> broken.lastMessage(topic));
            assertThrows(RawdataException.class, () -> broken.metadata(topic).keys());
            assertThrows(RawdataException.class, () -> broken.metadata(topic).get("k"));
            RawdataMessage a = message("a");
            assertThrows(RawdataException.class, () -> broken.producer(topic).publish(a));
            try (RawdataConsumer consumer = broken.consumer(topic)) {
                assertThrows(RawdataException.class, () -> consumer.receive(5, TimeUnit.SECONDS));
            }
        }
    }

    // ---- topic names

    /**
     * Topics whose names differ only by case are different topics, with their own messages and metadata.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void topicsDifferingOnlyByCaseStayApart() throws InterruptedException {
        String upper = topic + "-Case";
        String lower = topic + "-case";
        client.producer(upper).publish(message("upper"));
        client.producer(lower).publish(message("lower"));
        client.metadata(upper).put("k", new byte[]{1});
        client.metadata(lower).put("k", new byte[]{2});
        try (RawdataConsumer consumer = client.consumer(upper)) {
            assertEquals(List.of("upper"), readAll(consumer));
        }
        try (RawdataConsumer consumer = client.consumer(lower)) {
            assertEquals(List.of("lower"), readAll(consumer));
        }
        assertArrayEquals(new byte[]{1}, client.metadata(upper).get("k"));
        assertArrayEquals(new byte[]{2}, client.metadata(lower).get("k"));
    }

    /**
     * Invalid topic names are refused by every method that takes a topic.
     *
     * @param name an invalid name
     */
    @ParameterizedTest
    @ValueSource(strings = {"", "..", "../escape", "a/b", "a b", "-leading-dash", ".hidden", "a\"; DROP TABLE x; --", "a$b", "a\\b",
            "this-name-is-far-too-long-0123456789-0123456789-0123456789-0123456789-0123456789-0123456789-0123456789"})
    public void invalidTopicNamesAreRefused(String name) {
        assertThrows(IllegalArgumentException.class, () -> client.producer(name));
        assertThrows(IllegalArgumentException.class, () -> client.consumer(name));
        assertThrows(IllegalArgumentException.class, () -> client.metadata(name));
        assertThrows(IllegalArgumentException.class, () -> client.lastMessage(name));
        assertThrows(IllegalArgumentException.class, () -> client.cursorOf(name, RawdataIds.generate(), true));
        assertThrows(IllegalArgumentException.class, () -> client.cursorOf(name, "p", true, 0, Duration.ZERO));
    }

    /**
     * Topic names at the edges of the rule work.
     *
     * @throws Exception on failure
     */
    @Test
    public void validTopicNamesAtTheEdgesWork() throws Exception {
        String longest = "a".repeat(100);
        for (String name : List.of("a", "Z9", "a.b_c-d", longest)) {
            try (RawdataProducer producer = client.producer(name)) {
                producer.publish(message("p-" + name.length(), 1));
            }
            assertEquals("p-" + name.length(), client.lastMessage(name).position());
        }
    }

    // ---- closing

    /**
     * After close, the client refuses every operation, and close can be repeated.
     */
    @Test
    public void closedClientRefusesEverything() {
        client.close();
        client.close();
        assertTrue(client.isClosed());
        assertThrows(RawdataClosedException.class, () -> client.producer(topic));
        assertThrows(RawdataClosedException.class, () -> client.consumer(topic));
        assertThrows(RawdataClosedException.class, () -> client.metadata(topic));
        assertThrows(RawdataClosedException.class, () -> client.lastMessage(topic));
        assertThrows(RawdataClosedException.class, () -> client.cursorOf(topic, RawdataIds.generate(), true));
        assertThrows(RawdataClosedException.class, () -> client.cursorOf(topic, "p", true, 0, Duration.ZERO));
    }

    /**
     * Closing the client closes what it created.
     */
    @Test
    public void closingTheClientClosesItsProducersConsumersAndMetadata() {
        RawdataProducer producer = client.producer(topic);
        RawdataConsumer consumer = client.consumer(topic);
        RawdataMetadataClient metadata = client.metadata(topic);
        client.close();
        assertTrue(producer.isClosed());
        assertTrue(consumer.isClosed());
        assertThrows(RawdataClosedException.class, () -> producer.publish(message("a", 1)));
        assertThrows(RawdataClosedException.class, () -> consumer.receive(0, TimeUnit.SECONDS));
        assertThrows(RawdataClosedException.class, () -> consumer.seek(0));
        assertThrows(RawdataClosedException.class, metadata::keys);
        assertThrows(RawdataClosedException.class, () -> metadata.get("k"));
        assertThrows(RawdataClosedException.class, () -> metadata.put("k", new byte[1]));
        assertThrows(RawdataClosedException.class, () -> metadata.remove("k"));
    }

    /**
     * A closed producer refuses to publish; the client and its other producers go on.
     */
    @Test
    public void closedProducerRefusesToPublish() {
        RawdataProducer producer = client.producer(topic);
        producer.close();
        producer.close();
        assertTrue(producer.isClosed());
        assertFalse(client.isClosed());
        assertThrows(RawdataClosedException.class, () -> producer.publish(message("a", 1)));
        publish("b");
        assertEquals("b", client.lastMessage(topic).position());
    }

    /**
     * A closed consumer refuses to receive, even when messages are available.
     */
    @Test
    public void closedConsumerRefusesToReceive() {
        publish("a");
        RawdataConsumer consumer = client.consumer(topic);
        consumer.close();
        consumer.close();
        assertTrue(consumer.isClosed());
        assertThrows(RawdataClosedException.class, () -> consumer.receive(0, TimeUnit.SECONDS));
        assertThrows(RawdataClosedException.class, () -> consumer.seek(0));
    }

    /**
     * Closing a consumer wakes a receive that waits, which then throws.
     *
     * @throws Exception on failure
     */
    @Test
    public void closeWakesAWaitingReceive() throws Exception {
        RawdataConsumer consumer = client.consumer(topic);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch waiting = new CountDownLatch(1);
        Thread receiver = new Thread(() -> {
            waiting.countDown();
            try {
                consumer.receive(60, TimeUnit.SECONDS);
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        receiver.start();
        waiting.await();
        sleep(200);
        long closedAt = System.nanoTime();
        consumer.close();
        receiver.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(receiver.isAlive(), "receive must return after close");
        assertTrue(System.nanoTime() - closedAt < TimeUnit.SECONDS.toNanos(30));
        assertTrue(thrown.get() instanceof RawdataClosedException, "receive must throw RawdataClosedException, threw " + thrown.get());
    }

    /**
     * Closing the client wakes a receive that waits on one of its consumers, which then throws.
     *
     * @throws Exception on failure
     */
    @Test
    public void closingTheClientWakesAWaitingReceive() throws Exception {
        RawdataConsumer consumer = client.consumer(topic);
        CompletableFuture<Throwable> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                consumer.receive(60, TimeUnit.SECONDS);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
        sleep(200);
        client.close();
        Throwable thrown = waiting.get(30, TimeUnit.SECONDS);
        assertTrue(thrown instanceof RawdataClosedException, "receive must throw RawdataClosedException, threw " + thrown);
    }

    /**
     * Closing the consumer completes a pending asynchronous receive exceptionally.
     *
     * @throws Exception on failure
     */
    @Test
    public void closingTheConsumerFailsAPendingReceiveAsync() throws Exception {
        RawdataConsumer consumer = client.consumer(topic);
        CompletableFuture<? extends RawdataMessage> pending = consumer.receiveAsync();
        consumer.close();
        assertClosed(pending);
    }

    /**
     * Closing the client completes a pending asynchronous receive exceptionally, and asynchronous calls after the
     * close fail.
     *
     * @throws Exception on failure
     */
    @Test
    public void closingTheClientFailsAsynchronousCalls() throws Exception {
        RawdataConsumer consumer = client.consumer(topic);
        RawdataProducer producer = client.producer(topic);
        CompletableFuture<? extends RawdataMessage> pending = consumer.receiveAsync();
        client.close();
        assertClosed(pending);
        RawdataMessage message = message("a", 1);
        CompletableFuture<Void> published;
        try {
            published = producer.publishAsync(message);
        } catch (RawdataClosedException e) {
            return; // refusing at once is as good as a failed future
        }
        assertClosed(published);
    }

    /**
     * A closed consumer refuses a batch receive.
     */
    @Test
    public void closedConsumerRefusesABatchReceive() {
        RawdataConsumer consumer = client.consumer(topic);
        consumer.close();
        assertThrows(RawdataClosedException.class, () -> consumer.receiveUpTo(10, Duration.ZERO));
    }

    private static void assertClosed(CompletableFuture<?> future) throws InterruptedException {
        try {
            future.get(30, TimeUnit.SECONDS);
            throw new AssertionError("the future must fail with RawdataClosedException, it completed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RawdataClosedException, "expected RawdataClosedException, got " + e.getCause());
        } catch (TimeoutException e) {
            throw new AssertionError("the future must fail with RawdataClosedException, it did not complete", e);
        }
    }

    // ---- copying and batches

    /**
     * {@link RawdataCopy} copies a topic of this provider to another topic, keeping the ids and the order.
     *
     * @throws Exception on failure
     */
    @Test
    public void copyKeepsIdsAndOrder() throws Exception {
        for (int i = 0; i < 40; i++) {
            publish("m" + i);
        }
        String target = topic + "-copy";
        try (RawdataConsumer from = client.consumer(topic); RawdataProducer to = client.producer(target)) {
            assertEquals(40, RawdataCopy.copy(from, to, 7, Duration.ZERO));
        }
        List<RawdataMessage> source = new ArrayList<>();
        List<RawdataMessage> copy = new ArrayList<>();
        try (RawdataConsumer consumer = client.consumer(topic)) {
            source.addAll(consumer.receiveUpTo(1000, Duration.ofSeconds(1)));
        }
        try (RawdataConsumer consumer = client.consumer(target)) {
            copy.addAll(consumer.receiveUpTo(1000, Duration.ofSeconds(1)));
        }
        assertEquals(40, source.size());
        assertEquals(source, copy);
    }

    /**
     * A batch receive returns at most the number asked for, in order, and continues where it stopped.
     *
     * @throws Exception on failure
     */
    @Test
    public void receiveUpToReturnsAtMostTheNumberAskedFor() throws Exception {
        publish("a", "b", "c", "d", "e");
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(List.of("a", "b"), consumer.receiveUpTo(2, Duration.ofSeconds(5)).stream().map(RawdataMessage::position).toList());
            assertEquals(List.of("c", "d", "e"), consumer.receiveUpTo(10, Duration.ofSeconds(5)).stream().map(RawdataMessage::position).toList());
            assertEquals(List.of(), consumer.receiveUpTo(10, Duration.ofMillis(100)));
        }
    }

    /**
     * Every id a provider assigns is a UUIDv7, and its time is when it was published.
     */
    @Test
    public void assignedIdsAreUuidV7() {
        long before = System.currentTimeMillis();
        publish("a");
        long after = System.currentTimeMillis();
        RawdataMessage last = client.lastMessage(topic);
        assertTrue(RawdataIds.isValid(last.id()));
        assertTrue(before <= last.timestamp() && last.timestamp() <= after);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
