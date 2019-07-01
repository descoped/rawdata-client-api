package no.ssb.rawdata.api.storage;

public interface RawdataProducer {

    /**
     * Get the last position for stream
     *
     * @return
     */
    String lastPosition();

    void write(RawdataMessageContent content);

    void publish();

}
