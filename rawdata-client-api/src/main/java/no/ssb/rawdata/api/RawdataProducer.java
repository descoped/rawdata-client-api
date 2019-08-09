package no.ssb.rawdata.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RawdataProducer extends AutoCloseable {

    /**
     * @return the topic on which this producer will publish messages.
     */
    String topic();

    /**
     * Will read the last message in the stream and extract the externalId from
     * the payload of the message.
     *
     * @return the externalId of the last message in the stream
     * @throws RawdataClosedException if the producer was closed before or is closed during this call.
     */
    String lastExternalId() throws RawdataClosedException;

    /**
     * Constructs a builder that can be used to build the content of a message.
     *
     * @return the builder
     * @throws RawdataClosedException if the producer was closed before this call.
     */
    RawdataMessageContent.Builder builder() throws RawdataClosedException;

    /**
     * Buffer the content of a message, preparing it for publication to rawdata using one of the publish methods.
     *
     * @param builder a builder used to get the content that will be buffered
     * @return the content that was buffered
     * @throws RawdataClosedException if the producer was closed before this call.
     */
    RawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException;

    /**
     * Buffer the content of a message, preparing it for publication to rawdata using one of the publish methods.
     *
     * @param content the content that will be buffered
     * @return the content parameter which is the content that was buffered
     * @throws RawdataClosedException if the producer was closed before this call.
     */
    RawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException;

    /**
     * Publish all buffered content that matches any of the external-ids here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param externalIds a list of external-ids
     * @return the list of messages that was published.
     * @throws RawdataClosedException             if the producer was closed before or during this call.
     * @throws RawdataContentNotBufferedException if one or more of the external-ids provided by the externalIds param
     *                                            was not buffered before calling publish.
     */
    List<? extends RawdataMessageId> publish(List<String> externalIds) throws RawdataClosedException, RawdataContentNotBufferedException;

    /**
     * Publish all buffered content that matches any of the external-ids here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param externalIds a list of external-ids
     * @return the list of messages that was published.
     * @throws RawdataClosedException             if the producer was closed before or during this call.
     * @throws RawdataContentNotBufferedException if one or more of the external-ids provided by the externalIds param
     *                                            was not buffered before calling publish.
     */
    List<? extends RawdataMessageId> publish(String... externalIds) throws RawdataClosedException, RawdataContentNotBufferedException;

    /**
     * Asynchronously publish all buffered content that matches any of the external-ids here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param externalIds a list of external-ids
     * @return a list of completable futures representing each of the asynchronous operations.
     */
    List<CompletableFuture<? extends RawdataMessageId>> publishAsync(String... externalIds);

    /**
     * Returns whether or not the producer is closed.
     *
     * @return whether the producer is closed.
     */
    boolean isClosed();
}
