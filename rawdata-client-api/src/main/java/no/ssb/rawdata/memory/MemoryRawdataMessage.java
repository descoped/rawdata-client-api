package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Objects;

public class MemoryRawdataMessage implements RawdataMessage {
    private final MemoryRawdataMessageId id;
    private final MemoryRawdataMessageContent content;

    public MemoryRawdataMessage(MemoryRawdataMessageId id, MemoryRawdataMessageContent content) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        if (content == null) {
            throw new IllegalArgumentException("content cannot be null");
        }
        this.id = id;
        this.content = content;
    }

    @Override
    public MemoryRawdataMessageId id() {
        return id;
    }

    @Override
    public MemoryRawdataMessageContent content() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemoryRawdataMessage that = (MemoryRawdataMessage) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "MemoryRawdataMessage{" +
                "id=" + id +
                ", content=" + content +
                '}';
    }
}
