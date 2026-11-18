package io.descoped.rawdata.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The message the builder creates. Immutable: it holds its own copies of the content and hands out copies.
 */
final class RawdataDefaultMessage implements RawdataMessage {

    private final UUID id;
    private final String orderingGroup;
    private final long sequenceNumber;
    private final String position;
    private final Map<String, byte[]> data; // own copies, never handed out

    private RawdataDefaultMessage(Builder builder) {
        if (builder.position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        this.id = builder.id;
        this.orderingGroup = builder.orderingGroup;
        this.sequenceNumber = builder.sequenceNumber;
        this.position = requireNoNul(builder.position, "The position");
        requireNoNul(builder.orderingGroup, "The ordering group");
        Map<String, byte[]> copy = new LinkedHashMap<>();
        builder.data.forEach((key, value) -> copy.put(key, value.clone()));
        this.data = Collections.unmodifiableMap(copy);
    }

    /**
     * Refuses a NUL character, which a PostgreSQL text column cannot hold, so that every provider takes the same
     * strings.
     */
    static String requireNoNul(String value, String what) {
        if (value != null && value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(what + " must not contain the NUL character (U+0000)");
        }
        return value;
    }

    @Override
    public RawdataMessage.Builder copy() {
        return new Builder(this);
    }

    @Override
    public UUID id() {
        return id;
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
        byte[] value = data.get(key);
        return value == null ? null : value.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RawdataDefaultMessage)) {
            return false;
        }
        RawdataDefaultMessage that = (RawdataDefaultMessage) o;
        if (sequenceNumber != that.sequenceNumber
                || !Objects.equals(id, that.id)
                || !Objects.equals(orderingGroup, that.orderingGroup)
                || !position.equals(that.position)
                || !data.keySet().equals(that.data.keySet())) {
            return false;
        }
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            if (!Arrays.equals(entry.getValue(), that.data.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(id, orderingGroup, sequenceNumber, position);
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            hash = 31 * hash + entry.getKey().hashCode() * 17 + Arrays.hashCode(entry.getValue());
        }
        return hash;
    }

    @Override
    public String toString() {
        return "RawdataMessage{id=" + id
                + ", orderingGroup='" + orderingGroup + '\''
                + ", sequenceNumber=" + sequenceNumber
                + ", position='" + position + '\''
                + ", keys=" + data.keySet()
                + '}';
    }

    static final class Builder implements RawdataMessage.Builder {
        private String orderingGroup;
        private UUID id;
        private long sequenceNumber;
        private String position;
        private final Map<String, byte[]> data = new LinkedHashMap<>(); // own copies

        Builder() {
        }

        private Builder(RawdataDefaultMessage source) {
            orderingGroup = source.orderingGroup;
            id = source.id;
            sequenceNumber = source.sequenceNumber;
            position = source.position;
            data.putAll(source.data); // the message's arrays are never changed, so sharing them here is safe
        }

        @Override
        public RawdataMessage.Builder id(UUID id) {
            this.id = id == null ? null : RawdataIds.requireValid(id);
            return this;
        }

        @Override
        public UUID id() {
            return id;
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
            if (sequenceNumber < 0) {
                throw new IllegalArgumentException("sequenceNumber must be 0 or greater: " + sequenceNumber);
            }
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
            requireNoNul(Objects.requireNonNull(key, "key"), "A content key");
            Objects.requireNonNull(payload, "payload");
            data.put(key, payload.clone());
            return this;
        }

        @Override
        public Map<String, byte[]> data() {
            Map<String, byte[]> copy = new LinkedHashMap<>();
            data.forEach((key, value) -> copy.put(key, value.clone()));
            return copy;
        }

        @Override
        public Set<String> keys() {
            return Collections.unmodifiableSet(new LinkedHashSet<>(data.keySet()));
        }

        @Override
        public byte[] get(String key) {
            byte[] value = data.get(key);
            return value == null ? null : value.clone();
        }

        @Override
        public RawdataMessage build() {
            return new RawdataDefaultMessage(this);
        }
    }
}
