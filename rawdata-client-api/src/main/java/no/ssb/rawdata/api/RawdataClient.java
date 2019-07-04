package no.ssb.rawdata.api;

public interface RawdataClient {

    RawdataProducer producer(String topic);

    RawdataConsumer consumer(String topic, String subscription);

}
