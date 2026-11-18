package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataTopics;
import io.descoped.rawdata.api.RawdataWriterConflictException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A client that keeps topics as Avro files in an {@link AvroStore}. A producer buffers on a local disk and uploads one
 * large Avro file, a segment, per upload window ({@link AvroUploadWindow}); its messages are visible to consumers once
 * the window is uploaded. The segments of a topic are numbered without gaps, and a consumer reads a topic segment by
 * segment, polling for the next.
 * <p>
 * Only one process writes a topic. When this client first publishes to a topic it reads where the topic ends; after
 * that it uploads each window as the segment after the last, and the store creates a segment only if it does not
 * exist. If it does, another process wrote the topic: the client fails with {@link RawdataWriterConflictException}
 * and keeps failing to publish to the topic.
 * <p>
 * Closing a producer uploads its topic's open window and waits for the uploads; closing the client does that for
 * every topic. That is how an application shuts down gracefully, for example on SIGTERM.
 * <p>
 * Providers built on this engine, such as {@code filesystem} and {@code gcs}, create it with their store; it owns the
 * store and closes it on {@link #close()}.
 */
public final class AvroRawdataClient implements RawdataClient {

    private static final AtomicInteger THREADS = new AtomicInteger();

    private final AvroStore store;
    private final AvroUploadWindow window;
    private final long pollIntervalMillis;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Object> open = ConcurrentHashMap.newKeySet(); // producers and consumers
    private final Map<String, Tail> tails = new ConcurrentHashMap<>(); // the last segment seen of each topic
    private final Map<String, AvroTopicWriter> writers = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> publishLocks = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "rawdata-avro-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "rawdata-avro-window-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    /**
     * The last segment of a topic and the id of its last message; segment -1 for an empty topic.
     */
    record Tail(long segment, UUID lastId) {
        static final Tail EMPTY = new Tail(-1, RawdataIds.beginningOfTime());
    }

    /**
     * Creates a client on a store.
     *
     * @param store              the store; the client closes it
     * @param window             how producers buffer before they upload
     * @param pollIntervalMillis how often a waiting consumer looks for a new segment, 1 or more
     */
    public AvroRawdataClient(AvroStore store, AvroUploadWindow window, long pollIntervalMillis) {
        this.store = Objects.requireNonNull(store, "store");
        this.window = Objects.requireNonNull(window, "window");
        if (pollIntervalMillis < 1) {
            throw new IllegalArgumentException("The poll interval must be 1 ms or more: " + pollIntervalMillis);
        }
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public RawdataProducer producer(String topic) {
        AvroRawdataProducer producer = new AvroRawdataProducer(this, checked(topic));
        open.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        String name = checked(topic);
        if (cursor != null && !(cursor instanceof AvroCursor)) {
            throw new IllegalArgumentException("Not a cursor of an Avro provider: " + cursor.getClass().getName());
        }
        AvroRawdataConsumer consumer = new AvroRawdataConsumer(this, name, (AvroCursor) cursor, pollIntervalMillis);
        open.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, UUID id, boolean inclusive) {
        checked(topic);
        return new AvroCursor(RawdataIds.requireValid(id), inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        String name = checked(topic);
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(tolerance, "tolerance");
        UUID lowerBound = RawdataIds.beginningOf(approxTimestamp - tolerance.toMillis());
        UUID upperBound = RawdataIds.beginningOf(approxTimestamp + tolerance.toMillis() + 1);
        for (long segment = firstSegmentReaching(name, lowerBound); ; segment++) {
            List<RawdataMessage> messages = segment(name, segment);
            if (messages == null) {
                break;
            }
            for (RawdataMessage message : messages) {
                if (RawdataIds.ORDER.compare(message.id(), upperBound) >= 0) {
                    throw new RawdataNoSuchPositionException("Position not found in topic " + name + ": " + position);
                }
                if (RawdataIds.ORDER.compare(message.id(), lowerBound) >= 0 && position.equals(message.position())) {
                    return new AvroCursor(message.id(), inclusive);
                }
            }
        }
        throw new RawdataNoSuchPositionException("Position not found in topic " + name + ": " + position);
    }

    @Override
    public RawdataMessage lastMessage(String topic) {
        String name = checked(topic);
        Tail tail = tail(name);
        if (tail.segment() < 0) {
            return null;
        }
        return lastOf(name, tail.segment());
    }

    @Override
    public RawdataMetadataClient metadata(String topic) {
        return new AvroRawdataMetadataClient(this, checked(topic));
    }

    /**
     * Closes the topic's open upload window and waits until every window of the topic this client buffered is
     * uploaded and visible. Nothing to do if the client has not published to the topic.
     *
     * @param topic the topic
     * @throws RawdataException if an upload failed (the windows stay on disk), or another process writes the topic
     */
    public void flush(String topic) {
        AvroTopicWriter writer = writers.get(checked(topic));
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Closes the client: uploads every open window and waits for the uploads, then closes the producers, consumers
     * and store.
     *
     * @throws RawdataException if an upload failed; the client is closed, and the windows not uploaded stay on disk
     *                          for the next writer of the topic
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RawdataException failed = null;
        for (AvroTopicWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (RawdataException e) {
                failed = failed == null ? e : failed;
            }
        }
        writers.clear();
        for (Object producerOrConsumer : open) {
            if (producerOrConsumer instanceof AvroRawdataProducer producer) {
                producer.close();
            } else if (producerOrConsumer instanceof AvroRawdataConsumer consumer) {
                consumer.close();
            }
        }
        scheduler.shutdownNow();
        executor.shutdownNow();
        store.close();
        if (failed != null) {
            throw failed;
        }
    }

    // ---- for the producers, consumers and metadata clients

    AvroStore store() {
        return store;
    }

    ExecutorService executor() {
        return executor;
    }

    ScheduledExecutorService scheduler() {
        return scheduler;
    }

    /**
     * The writer of a topic, started on the first publish. Called with the topic's publish lock held.
     */
    AvroTopicWriter writer(String topic) {
        AvroTopicWriter writer = writers.get(topic);
        if (writer == null) {
            checkOpen();
            writer = AvroTopicWriter.start(this, topic, window);
            writers.put(topic, writer);
        }
        return writer;
    }

    /**
     * A producer closed: uploads its topic's open window and waits, as 2.x did.
     */
    void producerClosed(String topic) {
        AvroTopicWriter writer = writers.get(topic);
        if (writer != null && !isClosed()) {
            writer.flush();
        }
    }

    ReentrantLock publishLock(String topic) {
        return publishLocks.computeIfAbsent(topic, t -> new ReentrantLock());
    }

    void removed(Object producerOrConsumer) {
        open.remove(producerOrConsumer);
    }

    void checkOpen() {
        if (isClosed()) {
            throw new RawdataClosedException("client is closed");
        }
    }

    /**
     * A segment was uploaded: wakes this client's consumers of the topic, so they need not wait for their next poll.
     */
    void uploaded(String topic, Tail tail) {
        tails.merge(topic, tail, (seen, created) -> seen.segment() >= created.segment() ? seen : created);
        for (Object producerOrConsumer : open) {
            if (producerOrConsumer instanceof AvroRawdataConsumer consumer && consumer.topic().equals(topic)) {
                consumer.wake();
            }
        }
    }

    /**
     * Returns the topic's last segment as it is now, reading the store from the last segment seen.
     */
    Tail tail(String topic) {
        Tail seen = tails.getOrDefault(topic, Tail.EMPTY);
        long last = lastSegment(topic, seen.segment());
        if (last == seen.segment()) {
            return seen;
        }
        Tail tail = new Tail(last, lastOf(topic, last).id());
        tails.merge(topic, tail, (old, now) -> old.segment() >= now.segment() ? old : now);
        return tail;
    }

    /**
     * Reads a segment's messages.
     *
     * @return the messages, or null if there is no such segment
     */
    List<RawdataMessage> segment(String topic, long segment) {
        try {
            Optional<byte[]> content = store.read(topic, segment);
            return content.isPresent() ? AvroSegments.decode(content.get()) : null;
        } catch (IOException e) {
            throw failure("Reading segment " + segment + " of topic " + topic, e);
        }
    }

    /**
     * Returns the last message of a segment that exists, as every segment up to the last one seen does.
     */
    private RawdataMessage lastOf(String topic, long segment) {
        List<RawdataMessage> messages = segment(topic, segment);
        if (messages == null || messages.isEmpty()) {
            throw new RawdataException("Segment " + segment + " of topic " + topic
                    + " is missing or empty; segments are never removed or written empty, so the store was changed by hand");
        }
        return messages.get(messages.size() - 1);
    }

    /**
     * Returns the first segment whose last message has an id of at least the given one: where a reader of the ids
     * from there on starts. The last segment plus one if every message is before the id.
     */
    long firstSegmentReaching(String topic, UUID id) {
        if (RawdataIds.ORDER.compare(id, RawdataIds.beginningOfTime()) <= 0) {
            return 0;
        }
        long low = 0;
        long high = tail(topic).segment() + 1; // the answer is in [low, high]
        while (low < high) {
            long middle = (low + high) >>> 1;
            if (RawdataIds.ORDER.compare(lastOf(topic, middle).id(), id) >= 0) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return low;
    }

    /**
     * Finds the last segment of a topic, given one that exists (or -1). Segments are numbered without gaps, so it
     * probes forward in growing steps and then halves the step.
     */
    private long lastSegment(String topic, long known) {
        try {
            if (!store.exists(topic, known + 1)) {
                return known;
            }
            long low = known + 1; // exists
            long step = 1;
            long high; // does not exist
            for (; ; ) {
                long probe = low + step;
                if (!store.exists(topic, probe)) {
                    high = probe;
                    break;
                }
                low = probe;
                step <<= 1;
            }
            while (high - low > 1) {
                long middle = (low + high) >>> 1;
                if (store.exists(topic, middle)) {
                    low = middle;
                } else {
                    high = middle;
                }
            }
            return low;
        } catch (IOException e) {
            throw failure("Finding the last segment of topic " + topic, e);
        }
    }

    RawdataException failure(String what, IOException e) {
        if (isClosed()) {
            return new RawdataClosedException("client is closed");
        }
        return new RawdataException(what + " failed: " + e.getMessage(), e);
    }

    private String checked(String topic) {
        RawdataTopics.requireValid(topic);
        checkOpen();
        return topic;
    }

    @Override
    public String toString() {
        return "AvroRawdataClient{store=" + store + ", closed=" + isClosed() + '}';
    }
}
