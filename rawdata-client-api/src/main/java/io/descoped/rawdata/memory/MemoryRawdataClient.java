package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataTopics;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@code memory} provider's client: topics live in this client's memory and are gone when it is closed.
 */
final class MemoryRawdataClient implements RawdataClient {

    private static final AtomicInteger THREADS = new AtomicInteger();

    private final Map<String, MemoryRawdataTopic> topics = new ConcurrentHashMap<>();
    private final Set<Object> open = ConcurrentHashMap.newKeySet(); // producers and consumers
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "rawdata-memory-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public RawdataProducer producer(String topic) {
        MemoryRawdataProducer producer = new MemoryRawdataProducer(this, topic(topic));
        open.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        MemoryRawdataTopic memoryTopic = topic(topic);
        if (cursor != null && !(cursor instanceof MemoryCursor)) {
            throw new IllegalArgumentException("Not a cursor of the memory provider: " + cursor.getClass().getName());
        }
        MemoryRawdataConsumer consumer = new MemoryRawdataConsumer(this, memoryTopic, (MemoryCursor) cursor);
        open.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, UUID id, boolean inclusive) {
        topic(topic);
        return new MemoryCursor(RawdataIds.requireValid(id), inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(tolerance, "tolerance");
        MemoryRawdataTopic memoryTopic = topic(topic);
        UUID lowerBound = RawdataIds.beginningOf(approxTimestamp - tolerance.toMillis());
        UUID upperBound = RawdataIds.beginningOf(approxTimestamp + tolerance.toMillis() + 1);
        UUID id = memoryTopic.idOf(position, lowerBound, upperBound);
        if (id == null) {
            throw new RawdataNoSuchPositionException("Position not found in topic " + topic + ": " + position);
        }
        return new MemoryCursor(id, inclusive);
    }

    @Override
    public RawdataMessage lastMessage(String topic) {
        return topic(topic).lastMessage();
    }

    @Override
    public RawdataMetadataClient metadata(String topic) {
        return new MemoryRawdataMetadataClient(this, topic(topic));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Object producerOrConsumer : open) {
            if (producerOrConsumer instanceof RawdataProducer) {
                ((RawdataProducer) producerOrConsumer).close();
            } else {
                ((RawdataConsumer) producerOrConsumer).close();
            }
        }
        topics.values().forEach(MemoryRawdataTopic::wakeUp);
        executor.shutdownNow();
    }

    ExecutorService executor() {
        return executor;
    }

    void removed(Object producerOrConsumer) {
        open.remove(producerOrConsumer);
    }

    private MemoryRawdataTopic topic(String name) {
        RawdataTopics.requireValid(name);
        if (isClosed()) {
            throw new RawdataClosedException("client is closed");
        }
        return topics.computeIfAbsent(name, MemoryRawdataTopic::new);
    }
}
