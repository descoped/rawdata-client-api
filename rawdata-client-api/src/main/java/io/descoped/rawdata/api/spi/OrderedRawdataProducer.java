package io.descoped.rawdata.api.spi;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A producer for providers that write a call's messages in one step: the argument checks, asynchronous publish and
 * closing, and {@link #withIds}, which gives ids after the topic's greatest id and keeps given ones, as the contract
 * requires. A provider writes a batch in {@link #write}.
 */
public abstract class OrderedRawdataProducer implements RawdataProducer {

    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates the producer.
     *
     * @param topic the topic
     */
    protected OrderedRawdataProducer(String topic) {
        this.topic = topic;
    }

    /**
     * Writes a call's messages, all or none; typically: find the topic's greatest id, call {@link #withIds}, and write
     * the result.
     *
     * @param batch the messages, at least one, none null; the array is the producer's own copy
     */
    protected abstract void write(RawdataMessage[] batch);

    /**
     * Whether the client that created this producer is closed.
     *
     * @return true if it is
     */
    protected abstract boolean isClientClosed();

    /**
     * The executor of asynchronous publishes: the client's, which fails its tasks when the client closes.
     *
     * @return the executor
     */
    protected abstract Executor executor();

    /**
     * Called once, when the producer closes; for example, to tell the client.
     */
    protected void closed() {
        // nothing by default
    }

    @Override
    public final String topic() {
        return topic;
    }

    @Override
    public final void publish(RawdataMessage... messages) throws RawdataClosedException {
        Objects.requireNonNull(messages, "messages");
        for (RawdataMessage message : messages) {
            Objects.requireNonNull(message, "one of the messages is null");
        }
        checkOpen();
        if (messages.length > 0) {
            write(messages.clone());
        }
    }

    /**
     * Gives the messages without an id an id after the greatest id so far, starting after the topic's greatest id. A
     * given id is kept as it is, in any order: it may be lower than ids already in the topic.
     *
     * @param batch the messages
     * @param last  the topic's greatest id, or {@link RawdataIds#beginningOfTime()} for an empty topic
     * @return the messages as they are to be stored, in the order given, every one with an id
     */
    protected final List<RawdataMessage> withIds(RawdataMessage[] batch, UUID last) {
        UUID greatest = last;
        List<RawdataMessage> stored = new ArrayList<>(batch.length);
        for (RawdataMessage message : batch) {
            UUID id = message.id();
            if (id == null) {
                id = RawdataIds.next(greatest);
                stored.add(message.copy().id(id).build());
            } else {
                stored.add(message);
            }
            greatest = greatest(greatest, id);
        }
        return stored;
    }

    /**
     * The greater of two ids, in {@link RawdataIds#ORDER}.
     *
     * @param a an id
     * @param b another id
     * @return the greater one
     */
    protected static UUID greatest(UUID a, UUID b) {
        return RawdataIds.ORDER.compare(a, b) >= 0 ? a : b;
    }

    /**
     * The greatest id of stored messages and an id before them.
     *
     * @param last     the greatest id before the messages
     * @param messages the messages, every one with an id
     * @return the greatest id
     */
    protected static UUID greatest(UUID last, List<RawdataMessage> messages) {
        UUID greatest = last;
        for (RawdataMessage message : messages) {
            greatest = greatest(greatest, message.id());
        }
        return greatest;
    }

    @Override
    public final CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        RawdataMessage[] batch = Objects.requireNonNull(messages, "messages").clone();
        try {
            return CompletableFuture.runAsync(() -> publish(batch), executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closedException());
        }
    }

    @Override
    public final boolean isClosed() {
        return closed.get() || isClientClosed();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            closed();
        }
    }

    /**
     * Throws if the producer or its client is closed; for a {@link #write} that waits, to check again.
     *
     * @throws RawdataClosedException if closed
     */
    protected final void checkOpen() {
        if (isClosed()) {
            throw closedException();
        }
    }

    private RawdataClosedException closedException() {
        return new RawdataClosedException("producer of topic " + topic + " is closed");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{topic=" + topic + ", closed=" + isClosed() + '}';
    }
}
