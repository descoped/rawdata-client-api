package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataMessageId;

import java.util.Objects;

class MemoryRawdataMessageId implements RawdataMessageId {
    final String topic;
    final int index;

    MemoryRawdataMessageId(String topic, int index) {
        this.index = index;
        this.topic = topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryRawdataMessageId that = (MemoryRawdataMessageId) o;
        return index == that.index &&
                topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, index);
    }

    @Override
    public String toString() {
        return "MemoryRawdataMessageId{" +
                "topic='" + topic + '\'' +
                ", index=" + index +
                '}';
    }
}
