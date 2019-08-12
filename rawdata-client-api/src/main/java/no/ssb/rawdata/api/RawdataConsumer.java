package no.ssb.rawdata.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A subscription based consumer that can be used to receive and acknowledge messages on a stream.
 */
public interface RawdataConsumer extends AutoCloseable {

    /**
     * @return the name of the topic from which this consumer will consume messages from.
     */
    String topic();

    /**
     * Receive the next message after the current position, or null if no message is available before the timeout. If
     * successful, the current position for this consumer is updated to that of the returned message.
     *
     * @param timeout the timeout in units as specified by the unit parameter.
     * @param unit    the unit of the timeout, e.g. TimeUnit.SECONDS
     * @return the next available message before the timeout occurs, or null if no next message is available before the
     * timeout.
     * @throws InterruptedException   if the calling thread is interrupted while waiting on an available message.
     * @throws RawdataClosedException if method is called after this instance has been closed.
     */
    RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException;

    /**
     * Asynchronously receive a message callback when the next message after the current position is available. The
     * current position is also updated to that of the returned message right before calling the callback.
     *
     * @return a CompletableFuture representing the next available message.
     */
    CompletableFuture<? extends RawdataMessage> receiveAsync();

    /**
     * Returns whether or not the consumer is closed.
     *
     * @return whether the consumer is closed.
     */
    boolean isClosed();
}
