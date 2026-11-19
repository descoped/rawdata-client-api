package io.descoped.rawdata.api;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Reads the messages of one topic from a starting point, in the order of the {@link RawdataClient} contract: ids
 * published in increasing order are read in that order. A consumer keeps its own position and is meant for one thread
 * at a time.
 * <p>
 * Reading is pulled by the caller: nothing is delivered until the caller asks, so a caller takes only as much as it
 * can handle. {@link #receive(int, TimeUnit)} takes one message; {@link #receiveUpTo(int, Duration)} takes a batch of
 * a size the caller chooses.
 */
public interface RawdataConsumer extends AutoCloseable {

    /**
     * Returns the topic this consumer reads.
     *
     * @return the topic
     */
    String topic();

    /**
     * Receives the next message after the current position, waiting up to the timeout for one to be published. On
     * success the position moves to the returned message.
     *
     * @param timeout how long to wait; 0 returns at once
     * @param unit    the unit of the timeout
     * @return the next message, or null if none arrived before the timeout
     * @throws InterruptedException   if the thread is interrupted while waiting
     * @throws RawdataClosedException if the consumer or its client is closed, also while waiting
     */
    RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException;

    /**
     * Receives up to a number of messages: waits up to the timeout for the first, then takes the messages that are
     * available at once, until there are {@code maxMessages}. The position moves to the last message returned. If
     * the consumer is closed or the thread interrupted after the first message, the messages received so far are
     * returned (with the interrupt flag set again), so none is lost.
     *
     * @param maxMessages the most messages to return, 1 or more
     * @param timeout     how long to wait for the first message; zero returns at once
     * @return the messages, in order; empty if none arrived before the timeout
     * @throws IllegalArgumentException if maxMessages is less than 1 or the timeout is negative
     * @throws InterruptedException     if the thread is interrupted while waiting
     * @throws RawdataClosedException   if the consumer or its client is closed
     */
    default List<RawdataMessage> receiveUpTo(int maxMessages, Duration timeout) throws InterruptedException, RawdataClosedException {
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages must be 1 or more: " + maxMessages);
        }
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        List<RawdataMessage> messages = new ArrayList<>(Math.min(maxMessages, 1024));
        long millis = Math.min(timeout.toMillis(), Integer.MAX_VALUE);
        RawdataMessage message = receive((int) millis, TimeUnit.MILLISECONDS);
        while (message != null) {
            messages.add(message);
            if (messages.size() == maxMessages) {
                break;
            }
            try {
                message = receive(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RawdataClosedException e) {
                break;
            }
        }
        return messages;
    }

    /**
     * Receives the next message after the current position when it is available. The position moves to the message
     * before the future completes. At most one receive may be outstanding at a time.
     *
     * @return a future of the next message; it completes exceptionally with {@link RawdataClosedException} if the
     * consumer is closed before a message arrives
     */
    CompletableFuture<? extends RawdataMessage> receiveAsync();

    /**
     * Moves the position so that the next message received is the first one whose id has a time at or after a time.
     *
     * @param timestamp the time in milliseconds since 1970-01-01T00:00Z
     * @throws RawdataClosedException if the consumer or its client is closed
     */
    void seek(long timestamp);

    /**
     * Whether the consumer is closed.
     *
     * @return true after {@link #close()} on the consumer or its client
     */
    boolean isClosed();

    /**
     * Closes the consumer and wakes any receive that waits. Calling it again does nothing.
     */
    @Override
    void close();
}
