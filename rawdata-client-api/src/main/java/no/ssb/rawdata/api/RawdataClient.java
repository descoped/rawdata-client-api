package no.ssb.rawdata.api;

public interface RawdataClient extends AutoCloseable {

    RawdataProducer producer(String topic);

    RawdataConsumer consumer(String topic, String subscription);

    boolean isClosed();
}
