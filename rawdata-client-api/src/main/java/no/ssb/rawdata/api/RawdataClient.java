package no.ssb.rawdata.api;

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
        return consumer(topic, null);
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
    RawdataConsumer consumer(String topic, String initialPosition);

    /**
     * Returns whether or not the client is closed.
     *
     * @return whether the client is closed.
     */
    boolean isClosed();
}
