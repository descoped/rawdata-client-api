package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataMetadataClient;

import java.util.Objects;
import java.util.Set;

final class MemoryRawdataMetadataClient implements RawdataMetadataClient {

    private final MemoryRawdataClient client;
    private final MemoryRawdataTopic topic;

    MemoryRawdataMetadataClient(MemoryRawdataClient client, MemoryRawdataTopic topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic.name;
    }

    @Override
    public Set<String> keys() {
        checkOpen();
        return Set.copyOf(topic.metadata.keySet());
    }

    @Override
    public byte[] get(String key) {
        RawdataMetadataClient.requireValidKey(key);
        checkOpen();
        byte[] value = topic.metadata.get(key);
        return value == null ? null : value.clone();
    }

    @Override
    public RawdataMetadataClient put(String key, byte[] value) {
        RawdataMetadataClient.requireValidKey(key);
        Objects.requireNonNull(value, "value");
        checkOpen();
        topic.metadata.put(key, value.clone());
        return this;
    }

    @Override
    public RawdataMetadataClient remove(String key) {
        RawdataMetadataClient.requireValidKey(key);
        checkOpen();
        topic.metadata.remove(key);
        return this;
    }

    private void checkOpen() {
        if (client.isClosed()) {
            throw new RawdataClosedException("client is closed");
        }
    }
}
