package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class MemoryRawdataMessage implements RawdataMessage {

    final ULID.Value ulid;
    final String orderingGroup;
    final long sequenceNumber;
    final String position;
    final Map<String, byte[]> data;

    MemoryRawdataMessage(ULID.Value ulid, String orderingGroup, long sequenceNumber, String position, Map<String, byte[]> data) {
        if (ulid == null) {
            throw new IllegalArgumentException("ulid cannot be null");
        }
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.ulid = ulid;
        this.orderingGroup = orderingGroup;
        this.sequenceNumber = sequenceNumber;
        this.position = position;
        this.data = data;
    }

    @Override
    public ULID.Value ulid() {
        return ulid;
    }

    @Override
    public String orderingGroup() {
        return orderingGroup;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
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
        MemoryRawdataMessage that = (MemoryRawdataMessage) o;
        return sequenceNumber == that.sequenceNumber &&
                ulid.equals(that.ulid) &&
                orderingGroup.equals(that.orderingGroup) &&
                position.equals(that.position) &&
                data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ulid, orderingGroup, sequenceNumber, position, data);
    }

    static class Builder implements RawdataMessage.Builder {
        String orderingGroup;
        ULID.Value ulid;
        long sequenceNumber = 0;
        String position;
        Map<String, byte[]> data = new LinkedHashMap<>();

        @Override
        public RawdataMessage.Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public ULID.Value ulid() {
            return ulid;
        }

        @Override
        public RawdataMessage.Builder orderingGroup(String orderingGroup) {
            this.orderingGroup = orderingGroup;
            return this;
        }

        @Override
        public String orderingGroup() {
            return orderingGroup;
        }

        @Override
        public RawdataMessage.Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        @Override
        public long sequenceNumber() {
            return sequenceNumber;
        }


        @Override
        public RawdataMessage.Builder position(String position) {
            this.position = position;
            return this;
        }

        @Override
        public String position() {
            return position;
        }

        @Override
        public RawdataMessage.Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
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
        public MemoryRawdataMessage build() {
            return new MemoryRawdataMessage(ulid, orderingGroup, sequenceNumber, position, data);
        }
    }
}
