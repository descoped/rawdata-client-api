package io.descoped.rawdata.tck;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCopy;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The contract every {@link RawdataClient} provider keeps, as JUnit tests. A provider's test extends this class and
 * says how to configure a client; every test gets a fresh client and its own topic, so tests do not see each other's
 * messages even when the provider keeps data between clients.
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
        for (int round = 0; round < 20; round++) {
            try (RawdataProducer producer = client.producer(topic)) {
                producer.publish(
                        RawdataMessage.builder().id(RawdataIds.generate()).position("r" + round + "given").build(),
                        RawdataMessage.builder().position("r" + round + "generated").build());
            }
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            List<String> positions = readAll(consumer);
            assertEquals(40, positions.size());
            // A given id is placed by its value, which may sort before earlier messages; the generated id that
            // follows it in the same call must sort after it.
            for (int round = 0; round < 20; round++) {
                assertTrue(positions.indexOf("r" + round + "given") < positions.indexOf("r" + round + "generated"),
                        "round " + round + ": " + positions);
            }
        }
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

    // ---- topic names

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
