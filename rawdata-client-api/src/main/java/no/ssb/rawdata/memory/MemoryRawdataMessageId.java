package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Objects;

class MemoryRawdataMessageId {
    final String topic;
    final ULID.Value ulid;

    MemoryRawdataMessageId(String topic, ULID.Value ulid) {
        this.topic = topic;
        this.ulid = ulid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryRawdataMessageId that = (MemoryRawdataMessageId) o;
        return topic.equals(that.topic) &&
                ulid.equals(that.ulid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, ulid);
    }

    @Override
    public String toString() {
        return "MemoryRawdataMessageId{" +
                "topic='" + topic + '\'' +
                ", ulid=" + ulid +
                '}';
    }
}
