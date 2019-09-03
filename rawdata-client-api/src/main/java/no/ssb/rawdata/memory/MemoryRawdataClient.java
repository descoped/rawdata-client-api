package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        MemoryRawdataProducer producer = new MemoryRawdataProducer(topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t)), p -> {
            producers.remove(p);
        });
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        MemoryRawdataConsumer consumer = new MemoryRawdataConsumer(
                topicByName.computeIfAbsent(topic, t -> new MemoryRawdataTopic(t)),
                (MemoryCursor) cursor,
                c -> consumers.remove(c)
        );
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topicName, ULID.Value ulid, boolean inclusive) {
        return new MemoryCursor(ulid, inclusive, true);
    }

    @Override
    public MemoryCursor cursorOf(String topicName, String position, boolean inclusive) {
        MemoryRawdataTopic topic = topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t));
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            return new MemoryCursor(topic.ulidOf(position), inclusive, true);
        } finally {
            topic.unlock();
        }
    }

    @Override
    public RawdataMessage lastMessage(String topicName) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        MemoryRawdataTopic topic = topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t));
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
