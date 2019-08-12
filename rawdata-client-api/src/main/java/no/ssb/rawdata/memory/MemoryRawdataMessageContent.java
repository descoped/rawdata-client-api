package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MemoryRawdataMessageContent implements RawdataMessage {

    private final String position;
    private final Map<String, byte[]> data;

    public MemoryRawdataMessageContent(String position, Map<String, byte[]> data) {
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.position = position;
        this.data = data;
    }

    @Override
    public String position() {
        return position;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryRawdataMessageContent that = (MemoryRawdataMessageContent) o;
        return position.equals(that.position) &&
                data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, data);
    }

    @Override
    public String toString() {
        return "MemoryRawdataMessageContent{" +
                "position='" + position + '\'' +
                '}';
    }
}
