package io.descoped.rawdata.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes messages on one topic. A producer may be used from several threads.
 */
public interface RawdataProducer extends AutoCloseable {

    /**
     * Returns the topic this producer publishes on.
     *
     * @return the topic
     */
    String topic();

    /**
     * Publishes messages, in order. A message without an id is given one that is greater than every id already in
     * the topic. The messages of one call are written together: either all of them or none.
     *
     * @param messages the messages
     * @throws RawdataClosedException if the producer or its client is closed
     */
    default void publish(List<RawdataMessage> messages) throws RawdataClosedException {
        publish(messages.toArray(new RawdataMessage[0]));
    }

    /**
     * Publishes messages, in order. A message without an id is given one that is greater than every id already in
     * the topic. A message with an id keeps it, in any order: it may be lower than ids already in the topic, and a
     * consumer already past it does not get it. The messages of one call are written together: either all of them or
     * none.
     *
     * @param messages the messages
     * @throws RawdataClosedException if the producer or its client is closed
     */
    void publish(RawdataMessage... messages) throws RawdataClosedException;

    /**
     * Publishes messages asynchronously, as {@link #publish(RawdataMessage...)}.
     *
     * @param messages the messages
     * @return a future that completes when the messages are written, or exceptionally when the write fails
     */
    default CompletableFuture<Void> publishAsync(List<RawdataMessage> messages) {
        return publishAsync(messages.toArray(new RawdataMessage[0]));
    }

    /**
     * Publishes messages asynchronously, as {@link #publish(RawdataMessage...)}.
     *
     * @param messages the messages
     * @return a future that completes when the messages are written, or exceptionally when the write fails
     */
    CompletableFuture<Void> publishAsync(RawdataMessage... messages);

    /**
     * Whether the producer is closed.
     *
     * @return true after {@link #close()} on the producer or its client
     */
    boolean isClosed();

    /**
     * Closes the producer. Calling it again does nothing.
     */
    @Override
    void close();
}
