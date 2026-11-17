package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

final class MemoryRawdataProducer implements RawdataProducer {

    private final MemoryRawdataClient client;
    private final MemoryRawdataTopic topic;
    private final AtomicBoolean closed = new AtomicBoolean();

    MemoryRawdataProducer(MemoryRawdataClient client, MemoryRawdataTopic topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic.name;
    }

    @Override
    public void publish(RawdataMessage... messages) throws RawdataClosedException {
        Objects.requireNonNull(messages, "messages");
        for (RawdataMessage message : messages) {
            Objects.requireNonNull(message, "one of the messages is null");
        }
        if (isClosed()) {
            throw new RawdataClosedException("producer of topic " + topic.name + " is closed");
        }
        topic.publish(messages.clone());
    }

    @Override
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        RawdataMessage[] batch = Objects.requireNonNull(messages, "messages").clone();
        try {
            return CompletableFuture.runAsync(() -> publish(batch), client.executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(new RawdataClosedException("producer of topic " + topic.name + " is closed"));
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get() || client.isClosed();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.removed(this);
        }
    }
}
