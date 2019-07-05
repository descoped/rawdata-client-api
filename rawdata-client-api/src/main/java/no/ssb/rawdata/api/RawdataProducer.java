package no.ssb.rawdata.api;

import java.util.List;

public interface RawdataProducer extends AutoCloseable {

    /**
     * Get the last externalId in the stream
     *
     * @return
     */
    String lastExternalId();

    RawdataMessageContent.Builder builder();

    RawdataMessageContent buffer(RawdataMessageContent.Builder builder);

    RawdataMessageContent buffer(RawdataMessageContent content);

    List<? extends RawdataMessageId> publish(List<String> externalIds);

    List<? extends RawdataMessageId> publish(String... externalIds);
}
