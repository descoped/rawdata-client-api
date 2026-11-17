package io.descoped.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Optional.ofNullable;

public class MemoryRawdataClient implements RawdataClient {

    final Map<String, MemoryRawdataTopic> topicByName = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<MemoryRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<MemoryRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    @Override
    public MemoryRawdataProducer producer(String topicName) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        MemoryRawdataProducer producer = new MemoryRawdataProducer(topicByName.computeIfAbsent(topicName, MemoryRawdataTopic::new), producers::remove);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        MemoryRawdataConsumer consumer = new MemoryRawdataConsumer(
                topicByName.computeIfAbsent(topic, MemoryRawdataTopic::new),
                (MemoryCursor) cursor,
                consumers::remove
        );
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topicName, ULID.Value ulid, boolean inclusive) {
        return new MemoryCursor(ulid, inclusive, true);
    }

    @Override
    public RawdataCursor cursorOf(String topicName, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        MemoryRawdataTopic topic = topicByName.computeIfAbsent(topicName, MemoryRawdataTopic::new);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            ULID.Value lowerBound = RawdataConsumer.beginningOf(approxTimestamp - tolerance.toMillis());
            ULID.Value upperBound = RawdataConsumer.beginningOf(approxTimestamp + tolerance.toMillis());
            return ofNullable(topic.ulidOf(position, lowerBound, upperBound))
                    .map(ulid -> new MemoryCursor(ulid, inclusive, true))
                    .orElseThrow(() -> new RawdataNoSuchPositionException(String.format("Position not found: %s", position)));
        } finally {
            topic.unlock();
        }
    }

    @Override
    public RawdataMessage lastMessage(String topicName) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        MemoryRawdataTopic topic = topicByName.computeIfAbsent(topicName, MemoryRawdataTopic::new);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            return topic.lastMessage();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public MemoryRawdataMetadataClient metadata(String topic) {
        return new MemoryRawdataMetadataClient(topic);
    }

    @Override
    public void close() {
        for (MemoryRawdataProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        for (MemoryRawdataConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        closed.set(true);
    }
}
