package no.ssb.rawdata.api;

import de.huxhorn.sulky.ulid.ULID;

public interface RawdataClient extends AutoCloseable {

    /**
     * Create a new producer on the given topic. The producer can be used to produce messages on the topic stream.
     *
     * @param topic the name of the topic to produce messages on. Must be the context-specific short-name of the topic
     *              that is independent of any technology or implementation specific schemes which should be configured
     *              when loading the rawdata client provider.
     * @return
     */
    RawdataProducer producer(String topic);

    /**
     * Create a new consumer on the given topic, starting at the very beginning of the topic.
     *
     * @param topic the name of the topic to consume message from. Must be the context-specific short-name of the topic
     *              that is independent of any technology or implementation specific schemes which should be configured
     *              when loading the rawdata client provider.
     * @return a consumer that can be used to read the topic stream.
     */
    default RawdataConsumer consumer(String topic) {
        return consumer(topic, (ULID.Value) null, true);
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the rawdata client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @return a consumer that can be used to read the topic stream.
     */
    default RawdataConsumer consumer(String topic, ULID.Value initialPosition) {
        return consumer(topic, initialPosition, false);
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the rawdata client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @param inclusive       whether or not to include the message at the initial-position when reading the stream
     * @return a consumer that can be used to read the topic stream.
     */
    RawdataConsumer consumer(String topic, ULID.Value initialPosition, boolean inclusive);

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the rawdata client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @return a consumer that can be used to read the topic stream.
     */
    default RawdataConsumer consumer(String topic, String initialPosition) {
        return consumer(topic, initialPosition == null ? null : ulidOfPosition(topic, initialPosition), false);
    }

    /**
     * Create a new consumer on the given topic, starting at the given initial position (or at the beginning of the topic
     * if the given initial position is null).
     *
     * @param topic           the name of the topic to consume message from. Must be the context-specific short-name of
     *                        the topic that is independent of any technology or implementation specific schemes which
     *                        should be configured when loading the rawdata client provider.
     * @param initialPosition the position to be set as current position when creating the consumer
     * @param inclusive       whether or not to include the message at the initial-position when reading the stream
     * @return a consumer that can be used to read the topic stream.
     */
    default RawdataConsumer consumer(String topic, String initialPosition, boolean inclusive) {
        return consumer(topic, initialPosition == null ? null : ulidOfPosition(topic, initialPosition), inclusive);
    }

    /**
     * Find the ulid of the first message that matches the given position in the topic.
     *
     * @param topic    the topic
     * @param position the position to find
     * @return the ulid if a match is found, null otherwise
     */
    ULID.Value ulidOfPosition(String topic, String position);

    /**
     * Will read and return the last message in the stream.
     *
     * @param topic the name of the topic to read the last message position from.
     * @return the current last message in the stream
     * @throws RawdataClosedException if the producer was closed before or is closed during this call.
     */
    RawdataMessage lastMessage(String topic) throws RawdataClosedException;

    /**
     * Returns whether or not the client is closed.
     *
     * @return whether the client is closed.
     */
    boolean isClosed();
}
