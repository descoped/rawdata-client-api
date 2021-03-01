package no.ssb.rawdata.api;

import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RawdataFlows {

    public static Flow.Publisher<RawdataMessage> publisher(Supplier<RawdataConsumer> consumer) {
        return new RawdataPublisher(consumer);
    }

    public static Flow.Subscriber<RawdataMessage.Builder> subscriber(Supplier<RawdataProducer> producer) {
        return new RawdataSubscriber(producer);
    }

    public static Flow.Subscriber<RawdataMessage.Builder> subscriber(Supplier<RawdataProducer> producer, Consumer<RawdataProducer> onComplete) {
        return new RawdataSubscriber(producer, onComplete);
    }

    public static Flow.Subscriber<RawdataMessage.Builder> subscriber(Supplier<RawdataProducer> producer, BiConsumer<Throwable, RawdataProducer> onError) {
        return new RawdataSubscriber(producer, onError);
    }

    public static Flow.Subscriber<RawdataMessage.Builder> subscriber(Supplier<RawdataProducer> producer, Consumer<RawdataProducer> onComplete, BiConsumer<Throwable, RawdataProducer> onError) {
        return new RawdataSubscriber(producer, onComplete, onError);
    }
}
