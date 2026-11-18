package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataWriterConflictException;
import io.descoped.rawdata.api.spi.OrderedRawdataProducer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes a call's messages in one transaction that holds the topic's advisory lock: it reads the topic's last id,
 * gives new ids after it and checks given ids against it, so a consumer that has read up to an id never misses a
 * message committed later. Only one process writes a topic: if the last id is not the last this client wrote (after
 * its first publish to the topic), another process wrote, and the publish fails with
 * {@link RawdataWriterConflictException}, writing nothing.
 */
final class PostgresRawdataProducer extends OrderedRawdataProducer {

    private final PostgresRawdataClient client;

    PostgresRawdataProducer(PostgresRawdataClient client, String topic) {
        super(topic);
        this.client = client;
    }

    @Override
    protected void write(RawdataMessage[] batch) {
        String topic = topic();
        ReentrantLock lock = client.publishLock(topic);
        lock.lock();
        try {
            UUID wrote = client.jdbc().transaction(connection -> {
                lockTopic(connection, topic);
                UUID stored = PostgresMessages.lastId(connection, topic);
                UUID last = stored == null ? RawdataIds.beginningOfTime() : stored;
                UUID known = client.written(topic);
                if (known != null && !known.equals(last)) {
                    throw new RawdataWriterConflictException(topic); // another process published since; rolled back
                }
                List<RawdataMessage> messages = withIds(batch, last);
                PostgresMessages.insert(connection, topic, messages);
                return messages.get(messages.size() - 1).id();
            });
            client.written(topic, wrote);
        } finally {
            lock.unlock();
        }
    }

    private static void lockTopic(Connection connection, String topic) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, hashtext(?))")) {
            lock.setInt(1, PostgresSchema.TOPIC_LOCK);
            lock.setString(2, topic);
            lock.execute();
        }
    }

    @Override
    protected boolean isClientClosed() {
        return client.isClosed();
    }

    @Override
    protected Executor executor() {
        return client.executor();
    }

    @Override
    protected void closed() {
        client.removed(this);
    }
}
