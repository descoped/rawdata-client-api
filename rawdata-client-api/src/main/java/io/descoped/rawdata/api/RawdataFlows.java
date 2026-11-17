package io.descoped.rawdata.api;

import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * {@link Flow} adapters: a publisher that reads a topic through a consumer, and a subscriber that publishes what
 * it receives through a producer. Connected, they copy a topic.
 */
public class RawdataFlows {

    private RawdataFlows() {
    }

    /**
     * A publisher that emits every message of a topic, from a consumer created on subscription, and completes
     * when no more messages are available.
     *
     * @param consumer creates the consumer to read from
     * @return the publisher
     */
    public static Flow.Publisher<RawdataMessage> publisher(Supplier<RawdataConsumer> consumer) {
        return new RawdataPublisher(consumer);
    }

    /**
     * A subscriber that publishes every message it receives.
     *
     * @param producer creates the producer to publish with
     * @return the subscriber
     */
    public static Flow.Subscriber<RawdataMessage> subscriber(Supplier<RawdataProducer> producer) {
        return new RawdataSubscriber(producer);
    }

    /**
     * A subscriber that publishes every message it receives, and calls back on completion.
     *
     * @param producer   creates the producer to publish with
     * @param onComplete called with the producer when the publisher completes
     * @return the subscriber
     */
    public static Flow.Subscriber<RawdataMessage> subscriber(Supplier<RawdataProducer> producer, Consumer<RawdataProducer> onComplete) {
        return new RawdataSubscriber(producer, onComplete);
    }

    /**
     * A subscriber that publishes every message it receives, and calls back on error.
     *
     * @param producer creates the producer to publish with
     * @param onError  called with the error and the producer when the flow fails
     * @return the subscriber
     */
    public static Flow.Subscriber<RawdataMessage> subscriber(Supplier<RawdataProducer> producer, BiConsumer<Throwable, RawdataProducer> onError) {
        return new RawdataSubscriber(producer, onError);
    }

    /**
     * A subscriber that publishes every message it receives, and calls back on completion and on error.
     *
     * @param producer   creates the producer to publish with
     * @param onComplete called with the producer when the publisher completes
     * @param onError    called with the error and the producer when the flow fails
     * @return the subscriber
     */
    public static Flow.Subscriber<RawdataMessage> subscriber(Supplier<RawdataProducer> producer, Consumer<RawdataProducer> onComplete, BiConsumer<Throwable, RawdataProducer> onError) {
        return new RawdataSubscriber(producer, onComplete, onError);
    }
}
