package no.ssb.rawdata.api.storage;

import java.util.List;

public interface RawdataProducer {

    /**
     * Get the last position for stream
     *
     * @return
     */
    String lastPosition();

    RawdataMessageId write(RawdataMessageContent content);

    void publish(List<String> positions);

}
