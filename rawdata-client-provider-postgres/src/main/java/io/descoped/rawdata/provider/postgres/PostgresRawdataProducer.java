package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.OrderedRawdataProducer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Publishes a call's messages in one transaction that holds the topic's advisory lock: it reads the topic's last id,
 * gives new ids after it and checks given ids against it. Writers of a topic, in any process, so commit in id order,
 * and a consumer that has read up to an id never misses a message committed later.
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
        client.jdbc().transaction(connection -> {
            lockTopic(connection, topic);
            UUID last = PostgresMessages.lastId(connection, topic);
            PostgresMessages.insert(connection, topic, withIds(batch, last == null ? RawdataIds.beginningOfTime() : last));
            return null;
        });
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
