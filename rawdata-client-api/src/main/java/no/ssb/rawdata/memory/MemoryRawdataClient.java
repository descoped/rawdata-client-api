package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataMessageId;

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
        MemoryRawdataProducer producer = new MemoryRawdataProducer(topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t)), p -> {
            producers.remove(p);
        });
        this.producers.add(producer);
        return producer;
    }

    @Override
    public MemoryRawdataConsumer consumer(String topicName, RawdataMessageId initialPosition) {
        MemoryRawdataConsumer consumer = new MemoryRawdataConsumer(
                topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t)),
                initialPosition,
                c -> consumers.remove(c)
        );
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataMessageId findMessageId(String topicName, String externalId) {
        /*
         * Perform a full topic scan from start in an attempt to find the message with the given externalId
         */
        MemoryRawdataTopic topic = topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t));
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            MemoryRawdataMessageId pos = new MemoryRawdataMessageId(topicName, -1);
            while (topic.hasNext(pos)) {
                MemoryRawdataMessage message = topic.readNext(pos);
                if (externalId.equals(message.content().externalId())) {
                    return message.id();
                }
                pos = message.id();
            }
            return null;
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
