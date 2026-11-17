package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class MemoryRawdataConsumer implements RawdataConsumer {

    private final MemoryRawdataClient client;
    private final MemoryRawdataTopic topic;
    private final AtomicReference<MemoryCursor> position;
    private final AtomicBoolean closed = new AtomicBoolean();

    MemoryRawdataConsumer(MemoryRawdataClient client, MemoryRawdataTopic topic, MemoryCursor start) {
        this.client = client;
        this.topic = topic;
        this.position = new AtomicReference<>(start != null ? start : new MemoryCursor(RawdataIds.beginningOfTime(), true));
    }

    @Override
    public String topic() {
        return topic.name;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        checkOpen();
        MemoryCursor cursor = position.get();
        RawdataMessage message = topic.next(cursor, unit.toNanos(timeout), this::isClosed);
        checkOpen();
        if (message != null) {
            position.compareAndSet(cursor, new MemoryCursor(message.id(), false));
        }
        return message;
    }

    @Override
    public CompletableFuture<RawdataMessage> receiveAsync() {
        checkOpen();
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    for (; ; ) {
                        RawdataMessage message = receive(1, TimeUnit.SECONDS);
                        if (message != null) {
                            return message;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // closing the client interrupts its threads
                    throw new CompletionException(isClosed() ? closedException() : e);
                }
            }, client.executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closedException()); // the client closed meanwhile
        }
    }

    @Override
    public void seek(long timestamp) {
        checkOpen();
        position.set(new MemoryCursor(RawdataIds.beginningOf(timestamp), true));
    }

    @Override
    public boolean isClosed() {
        return closed.get() || client.isClosed();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.removed(this);
            topic.wakeUp();
        }
    }

    private void checkOpen() {
        if (isClosed()) {
            throw closedException();
        }
    }

    private RawdataClosedException closedException() {
        return new RawdataClosedException("consumer of topic " + topic.name + " is closed");
    }

    @Override
    public String toString() {
        return "MemoryRawdataConsumer{topic=" + topic.name + ", position=" + position.get() + ", closed=" + isClosed() + '}';
    }
}
