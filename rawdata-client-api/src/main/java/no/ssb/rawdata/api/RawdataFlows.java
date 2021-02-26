package no.ssb.rawdata.api;

import java.util.concurrent.Flow;
import java.util.function.Supplier;

public class RawdataFlows {

    public static Flow.Publisher<RawdataMessage> publisher(Supplier<RawdataConsumer> consumer) {
        return new RawdataPublisher(consumer);
    }

    public static Flow.Subscriber<RawdataMessage.Builder> subscriber(Supplier<RawdataProducer> producer) {
        return new RawdataSubscriber(producer);
    }
}
