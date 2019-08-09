package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryRawdataClient implements RawdataClient {

    final Map<String, MemoryRawdataTopic> topicByName = new ConcurrentHashMap<>();
    final Map<String, Map<String, MemoryRawdataConsumer>> subscriptionByNameByTopic = new ConcurrentHashMap<>();
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
    public MemoryRawdataConsumer consumer(String topicName, String subscription) {
        MemoryRawdataConsumer consumer = subscriptionByNameByTopic.computeIfAbsent(topicName, tn -> new ConcurrentHashMap<>())
                .computeIfAbsent(subscription, s ->
                        new MemoryRawdataConsumer(topicByName.computeIfAbsent(topicName, t ->
                                new MemoryRawdataTopic(t)), s,
                                c -> {
                                    subscriptionByNameByTopic.get(c.topic()).remove(c.subscription());
                                    consumers.remove(c);
                                })
                );
        consumers.add(consumer);
        return consumer;
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
