package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes a call's messages as one segment, the one after the topic's last. Ids are given after the last id of
 * the last segment, and given ids are checked against it. If another writer creates that segment first, the
 * producer reads the new last segment and tries again, so a call is written whole after everything before it, or
 * not at all.
 */
final class AvroRawdataProducer implements RawdataProducer {

    private final AvroRawdataClient client;
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean();

    AvroRawdataProducer(AvroRawdataClient client, String topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public void publish(RawdataMessage... messages) throws RawdataClosedException {
        Objects.requireNonNull(messages, "messages");
        for (RawdataMessage message : messages) {
            Objects.requireNonNull(message, "one of the messages is null");
        }
        checkOpen();
        if (messages.length == 0) {
            return;
        }
        RawdataMessage[] batch = messages.clone();
        // Producers of this client take turns, so they do not race each other for the next segment.
        ReentrantLock lock = client.publishLock(topic);
        lock.lock();
        try {
            // The last segment this client saw; if it is no longer the last, creating the next fails and says so.
            AvroRawdataClient.Tail tail = client.seenTail(topic);
            for (; ; ) {
                checkOpen();
                List<RawdataMessage> stored = withIds(batch, tail.lastId());
                long segment = tail.segment() + 1;
                if (client.store().create(topic, segment, AvroSegments.encode(stored))) {
                    client.published(topic, new AvroRawdataClient.Tail(segment, stored.get(stored.size() - 1).id()));
                    return;
                }
                tail = client.tail(topic); // another writer was first
            }
        } catch (IOException e) {
            throw client.failure("Publishing to topic " + topic, e);
        } finally {
            lock.unlock();
        }
    }

    private List<RawdataMessage> withIds(RawdataMessage[] batch, UUID last) {
        UUID previous = last;
        List<RawdataMessage> stored = new ArrayList<>(batch.length);
        for (RawdataMessage message : batch) {
            UUID id = message.id();
            if (id == null) {
                id = RawdataIds.next(previous);
                stored.add(message.copy().id(id).build());
            } else if (RawdataIds.ORDER.compare(id, previous) > 0) {
                stored.add(message);
            } else {
                throw new IllegalArgumentException("Id " + id + " of message at position " + message.position()
                        + " is not greater than the last id in topic " + topic + ": " + previous);
            }
            previous = id;
        }
        return stored;
    }

    @Override
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        RawdataMessage[] batch = Objects.requireNonNull(messages, "messages").clone();
        try {
            return CompletableFuture.runAsync(() -> publish(batch), client.executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closedException());
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

    private void checkOpen() {
        if (isClosed()) {
            throw closedException();
        }
    }

    private RawdataClosedException closedException() {
        return new RawdataClosedException("producer of topic " + topic + " is closed");
    }

    @Override
    public String toString() {
        return "AvroRawdataProducer{topic=" + topic + ", closed=" + isClosed() + '}';
    }
}
