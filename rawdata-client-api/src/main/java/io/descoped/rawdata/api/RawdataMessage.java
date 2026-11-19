package io.descoped.rawdata.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * One message in a topic: an id, an optional ordering group and sequence number, the source position, and named
 * binary content.
 * <p>
 * A message is immutable. Content goes in and comes out as copies: changing an array passed to
 * {@link Builder#put(String, byte[])} or returned by {@link #get(String)} never changes the message. Messages are
 * made by {@link #builder()} only, so a store can keep a message as it is.
 * <p>
 * The position, the ordering group and the content keys may be any string without the NUL character (U+0000), which
 * a PostgreSQL text column cannot hold, so every provider stores the same messages.
 */
public sealed interface RawdataMessage permits RawdataDefaultMessage {

    /**
     * Creates a builder for a new message.
     *
     * @return a new builder
     */
    static RawdataMessage.Builder builder() {
        return new RawdataDefaultMessage.Builder();
    }

    /**
     * Creates a builder that starts from this message.
     *
     * @return a builder with this message's values
     */
    RawdataMessage.Builder copy();

    /**
     * The unique id of the message, a UUIDv7 (see {@link RawdataIds}). The messages of a topic are read in the
     * {@link RawdataIds#ORDER order} of their ids.
     *
     * @return the id, or null for a message that was built without one and not yet published
     */
    UUID id();

    /**
     * The timestamp of the message, in milliseconds since 1970-01-01T00:00Z, taken from the id.
     *
     * @return the timestamp
     * @throws NullPointerException if the message has no id yet
     */
    default long timestamp() {
        return RawdataIds.timestamp(id());
    }

    /**
     * The group of messages that belong to the same sequence. If producers write the same ordering group in parallel,
     * they must keep the sequence numbers of the group strictly increasing by one, so that consumers can restore the
     * order.
     *
     * @return the ordering group, or null if none was set
     */
    String orderingGroup();

    /**
     * The number of the message in its ordering group: starts at 1 and increases by 1 without gaps, or 0 when the
     * topic itself guarantees the order.
     *
     * @return the sequence number, 0 or greater
     */
    long sequenceNumber();

    /**
     * The identifier of the message in its source system. It need not be sortable or unique.
     *
     * @return the position, never null
     */
    String position();

    /**
     * The content keys, in the order they were added.
     *
     * @return an unmodifiable set of the keys
     */
    Set<String> keys();

    /**
     * The content for a key.
     *
     * @param key the content key
     * @return a copy of the content, or null if the message has no such key
     */
    byte[] get(String key);

    /**
     * All content, by key, in the order the keys were added.
     *
     * @return a new map with copies of the content
     */
    default Map<String, byte[]> data() {
        Map<String, byte[]> data = new LinkedHashMap<>();
        for (String key : keys()) {
            data.put(key, get(key));
        }
        return data;
    }

    /**
     * Builds a message. The builder copies content when it is put and when the message is built, so the builder can
     * be reused and the built message never changes.
     */
    interface Builder {

        /**
         * Sets the ordering group, see {@link RawdataMessage#orderingGroup()}.
         *
         * @param orderingGroup the group, or null for none
         * @return this builder
         */
        Builder orderingGroup(String orderingGroup);

        /**
         * Returns the ordering group set so far.
         *
         * @return the group, or null if none has been set
         */
        String orderingGroup();

        /**
         * Sets the sequence number, see {@link RawdataMessage#sequenceNumber()}. 0, the default, means not used.
         *
         * @param sequenceNumber the sequence number, 0 or greater
         * @return this builder
         * @throws IllegalArgumentException if the sequence number is negative
         */
        Builder sequenceNumber(long sequenceNumber);

        /**
         * Returns the sequence number set so far.
         *
         * @return the sequence number
         */
        long sequenceNumber();

        /**
         * Sets the id. Without one, the producer assigns an id when it publishes the message; that id is greater
         * than every id already in the topic. A given id is kept as it is, in any order: it may be lower than ids
         * already in the topic. Ids must be unique, which is the caller's to keep. A copy of a message keeps its id.
         *
         * @param id a UUIDv7, or null to let the producer assign one
         * @return this builder
         * @throws IllegalArgumentException if the id is not a UUIDv7
         */
        Builder id(UUID id);

        /**
         * Returns the id set so far.
         *
         * @return the id, or null if none has been set
         */
        UUID id();

        /**
         * Sets the identifier of the message in its source system. Required.
         *
         * @param position the position
         * @return this builder
         */
        Builder position(String position);

        /**
         * Returns the position set so far.
         *
         * @return the position, or null if none has been set
         */
        String position();

        /**
         * Adds content under a key, replacing any content the key had. The builder keeps a copy of the array.
         *
         * @param key     the content key
         * @param payload the content
         * @return this builder
         * @throws NullPointerException     if the key or the payload is null
         * @throws IllegalArgumentException if the key has a NUL character (U+0000)
         */
        Builder put(String key, byte[] payload);

        /**
         * Adds all content of a map, as {@link #put(String, byte[])} for each entry.
         *
         * @param data the content by key
         * @return this builder
         */
        default Builder data(Map<String, byte[]> data) {
            data.forEach(this::put);
            return this;
        }

        /**
         * Returns the content added so far, by key.
         *
         * @return a new map with copies of the content
         */
        Map<String, byte[]> data();

        /**
         * Returns the content keys added so far.
         *
         * @return an unmodifiable copy of the keys
         */
        Set<String> keys();

        /**
         * Returns the content for a key.
         *
         * @param key the content key
         * @return a copy of the content, or null if no such key was added
         */
        byte[] get(String key);

        /**
         * Builds the message.
         *
         * @return the message
         * @throws IllegalArgumentException if the position is missing, or the position or the ordering group has a NUL
         *                                  character (U+0000)
         */
        RawdataMessage build();
    }
}
