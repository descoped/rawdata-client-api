package no.ssb.rawdata.api.storage;

public interface RawdataAPI {

    RawdataProducer producer(String stream);

    RawdataConsumer consumer(String stream, String subscription, SubscriptionMode mode);

}
