package io.descoped.rawdata.discard;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataTopics;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@code discard} provider's client: publishing accepts and drops every message, so every topic is always empty,
 * consumers never receive anything, and metadata is never kept. Topic names and the closed state are checked as in
 * every provider.
 */
final class DiscardingRawdataClient implements RawdataClient {

    private static final RawdataCursor CURSOR = new RawdataCursor() {
    };

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<DiscardingConsumer> consumers = ConcurrentHashMap.newKeySet();

    @Override
    public RawdataProducer producer(String topic) {
        String name = checked(topic);
        return new RawdataProducer() {
            private final AtomicBoolean producerClosed = new AtomicBoolean();

            @Override
            public String topic() {
                return name;
            }

            @Override
            public void publish(RawdataMessage... messages) {
                Objects.requireNonNull(messages, "messages");
                if (isClosed()) {
                    throw new RawdataClosedException("producer of topic " + name + " is closed");
                }
            }

            @Override
            public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
                try {
                    publish(messages);
                    return CompletableFuture.completedFuture(null);
                } catch (RuntimeException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }

            @Override
            public boolean isClosed() {
                return producerClosed.get() || closed.get();
            }

            @Override
            public void close() {
                producerClosed.set(true);
            }
        };
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        DiscardingConsumer consumer = new DiscardingConsumer(checked(topic));
        consumers.add(consumer);
        if (isClosed()) {
            consumer.close(); // the client closed while this consumer was created
        }
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, UUID id, boolean inclusive) {
        checked(topic);
        RawdataIds.requireValid(id);
        return CURSOR;
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        checked(topic);
        throw new RawdataNoSuchPositionException("Position not found in topic " + topic + " (the discard provider keeps nothing): " + position);
    }

    @Override
    public RawdataMessage lastMessage(String topic) {
        checked(topic);
        return null;
    }

    @Override
    public RawdataMetadataClient metadata(String topic) {
        String name = checked(topic);
        return new RawdataMetadataClient() {
            @Override
            public String topic() {
                return name;
            }

            @Override
            public Set<String> keys() {
                checkOpen();
                return Set.of();
            }

            @Override
            public byte[] get(String key) {
                Objects.requireNonNull(key, "key");
                checkOpen();
                return null; // NOSONAR null is the contract of RawdataMetadataClient.get for a missing entry
            }

            @Override
            public RawdataMetadataClient put(String key, byte[] value) {
                Objects.requireNonNull(key, "key");
                Objects.requireNonNull(value, "value");
                checkOpen();
                return this;
            }

            @Override
            public RawdataMetadataClient remove(String key) {
                Objects.requireNonNull(key, "key");
                checkOpen();
                return this;
            }
        };
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            consumers.forEach(DiscardingConsumer::close);
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new RawdataClosedException("client is closed");
        }
    }

    private String checked(String topic) {
        RawdataTopics.requireValid(topic);
        checkOpen();
        return topic;
    }

    /**
     * Never receives anything. Closing it, or its client, wakes a waiting receive and fails the futures of
     * {@link #receiveAsync()}.
     */
    private final class DiscardingConsumer implements RawdataConsumer {

        private final String name;
        private final AtomicBoolean consumerClosed = new AtomicBoolean();
        private final Set<CompletableFuture<RawdataMessage>> pending = ConcurrentHashMap.newKeySet();

        DiscardingConsumer(String name) {
            this.name = name;
        }

        @Override
        public String topic() {
            return name;
        }

        /**
         * Waits out the timeout (nothing is ever published) and returns null.
         */
        @Override
        public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException {
            checkOpen();
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            synchronized (this) {
                long remaining;
                while (!isClosed() && (remaining = deadline - System.nanoTime()) > 0) {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
            }
            checkOpen();
            return null;
        }

        /**
         * Returns a future that never completes with a message; closing the consumer or the client completes it
         * exceptionally with {@link RawdataClosedException}.
         */
        @Override
        public CompletableFuture<RawdataMessage> receiveAsync() {
            checkOpen();
            CompletableFuture<RawdataMessage> future = new CompletableFuture<>();
            pending.add(future);
            future.whenComplete((message, failure) -> pending.remove(future));
            if (isClosed()) {
                future.completeExceptionally(closedException());
            }
            return future;
        }

        @Override
        public void seek(long timestamp) {
            checkOpen();
        }

        @Override
        public boolean isClosed() {
            return consumerClosed.get() || closed.get();
        }

        @Override
        public void close() {
            if (!consumerClosed.compareAndSet(false, true)) {
                return;
            }
            consumers.remove(this);
            synchronized (this) {
                notifyAll();
            }
            for (CompletableFuture<RawdataMessage> future : pending) {
                future.completeExceptionally(closedException());
            }
        }

        private RawdataClosedException closedException() {
            return new RawdataClosedException("consumer of topic " + name + " is closed");
        }

        private void checkOpen() {
            if (isClosed()) {
                throw closedException();
            }
        }
    }
}
