package no.ssb.rawdata.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A subscription based consumer that can be used to receive and acknowledge messages on a stream.
 */
public interface RawdataConsumer extends AutoCloseable {

    /**
     * @return the topic from which this consumer will consume messages from.
     */
    String topic();

    /**
     * @return the name of the subscription on which this consumer is subscribed.
     */
    String subscription();

    /**
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     * @throws RawdataClosedException
     */
    RawdataMessage receive(int timeout, TimeUnit timeUnit) throws InterruptedException, RawdataClosedException;

    /**
     * Asynchronously receive a message callback when a new message is available on this consumer.
     *
     * @return a CompletableFuture representing the asynchronous operation.
     */
    CompletableFuture<? extends RawdataMessage> receiveAsync();

    /**
     * Acknowledge all messages up to and including the message identified by the given message-id. This allows for a
     * safe checkpoint on this subscription in case of failure where no message before (and including) the last
     * acknowledged message will be redelivered when continuing consumption on this subscription.
     *
     * @param id
     * @throws RawdataClosedException
     */
    void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException;

    /**
     * Returns whether or not the consumer is closed.
     *
     * @return whether the consumer is closed.
     */
    boolean isClosed();
}
