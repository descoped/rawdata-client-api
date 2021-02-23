package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataMetadataClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryRawdataMetadataClient implements RawdataMetadataClient {

    final String topic;
    final Map<String, byte[]> map = new ConcurrentHashMap<>();

    public MemoryRawdataMetadataClient(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        return map.keySet();
    }

    @Override
    public byte[] get(String key) {
        return map.get(key);
    }

    @Override
    public MemoryRawdataMetadataClient put(String key, byte[] value) {
        map.put(key, value);
        return this;
    }

    @Override
    public MemoryRawdataMetadataClient remove(String key) {
        map.remove(key);
        return this;
    }
}
