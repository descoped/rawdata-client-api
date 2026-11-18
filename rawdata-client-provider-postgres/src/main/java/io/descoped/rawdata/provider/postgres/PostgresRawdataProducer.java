package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Publishes a call's messages in one transaction that holds the topic's advisory lock: it reads the topic's last id,
 * gives new ids after it and checks given ids against it. Writers of a topic, in any process, so commit in id order,
 * and a consumer that has read up to an id never misses a message committed later.
 */
final class PostgresRawdataProducer implements RawdataProducer {

    private final PostgresRawdataClient client;
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean();

    PostgresRawdataProducer(PostgresRawdataClient client, String topic) {
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
        client.jdbc().transaction(connection -> {
            lockTopic(connection);
            UUID last = PostgresMessages.lastId(connection, topic);
            UUID previous = last == null ? RawdataIds.beginningOfTime() : last;
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
            PostgresMessages.insert(connection, topic, stored);
            return null;
        });
    }

    private void lockTopic(Connection connection) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, hashtext(?))")) {
            lock.setInt(1, PostgresSchema.TOPIC_LOCK);
            lock.setString(2, topic);
            lock.execute();
        }
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
}
