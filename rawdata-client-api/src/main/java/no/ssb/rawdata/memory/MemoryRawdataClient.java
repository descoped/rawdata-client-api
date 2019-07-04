package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryRawdataClient implements RawdataClient {

    final Map<String, MemoryRawdataTopic> topicByName = new ConcurrentHashMap<>();
    final Map<String, Map<String, MemoryRawdataConsumer>> subscriptionByNameByTopic = new ConcurrentHashMap<>();

    @Override
    public MemoryRawdataProducer producer(String topicName) {
        return new MemoryRawdataProducer(topicByName.computeIfAbsent(topicName, t -> new MemoryRawdataTopic(t)));
    }

    @Override
    public MemoryRawdataConsumer consumer(String topicName, String subscription) {
        return subscriptionByNameByTopic.computeIfAbsent(topicName, tn -> new ConcurrentHashMap<>())
                .computeIfAbsent(subscription, s ->
                        new MemoryRawdataConsumer(topicByName.computeIfAbsent(topicName, t ->
                                new MemoryRawdataTopic(t)), s, new MemoryRawdataMessageId(topicName, -1)
                        )
                );
    }

    @Override
    public void close() {
    }
}
