package no.ssb.rawdata.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RawdataProducer extends AutoCloseable {

    /**
     * @return the topic on which this producer will publish messages.
     */
    String topic();

    /**
     * Constructs a builder that can be used to build the content of a message.
     *
     * @return the builder
     * @throws RawdataClosedException if the producer was closed before this call.
     */
    RawdataMessage.Builder builder() throws RawdataClosedException;

    /**
     * Buffer the content of a message, preparing it for publication to rawdata using one of the publish methods.
     *
     * @param builder a builder used to build the message that will be buffered
     * @return this instance
     * @throws RawdataClosedException if the producer was closed before this call.
     */
    RawdataProducer buffer(RawdataMessage.Builder builder) throws RawdataClosedException;

    /**
     * Publish all buffered content that matches any of the positions here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param positions a list of positions
     * @throws RawdataClosedException             if the producer was closed before or during this call.
     * @throws RawdataNotBufferedException if one or more of the positions provided by the positions param
     *                                            was not buffered before calling publish.
     */
    default void publish(List<String> positions) throws RawdataClosedException, RawdataNotBufferedException {
        publish(positions.toArray(new String[positions.size()]));
    }

    /**
     * Publish all buffered content that matches any of the positions here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param positions a list of positions
     * @throws RawdataClosedException             if the producer was closed before or during this call.
     * @throws RawdataNotBufferedException if one or more of the positions provided by the positions param
     *                                            was not buffered before calling publish.
     */
    void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException;

    /**
     * Asynchronously publish all buffered content that matches any of the positions here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param positions a list of positions
     * @return a completable futures representing the completeness of the async-function.
     */
    default CompletableFuture<Void> publishAsync(List<String> positions) {
        return publishAsync(positions.toArray(new String[positions.size()]));
    }

    /**
     * Asynchronously publish all buffered content that matches any of the positions here provided, then remove those contents from
     * the buffer. Published content will be assigned a message-id that is available in the returned list of messages.
     *
     * @param positions a list of positions
     * @return a completable futures representing the completeness of the async-function.
     */
    CompletableFuture<Void> publishAsync(String... positions);

    /**
     * Returns whether or not the producer is closed.
     *
     * @return whether the producer is closed.
     */
    boolean isClosed();
}
