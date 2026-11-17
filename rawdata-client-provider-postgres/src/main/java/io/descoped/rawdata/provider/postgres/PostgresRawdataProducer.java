package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.provider.postgres.tx.Transaction;
import io.descoped.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class PostgresRawdataProducer implements RawdataProducer {

    private final TransactionFactory transactionFactory;
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<UUID> previousIdRef = new AtomicReference<>(RawdataIds.beginningOfTime());

    PostgresRawdataProducer(TransactionFactory transactionFactory, String topic) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public void publish(RawdataMessage... messages) throws RawdataClosedException {
        try (Transaction tx = transactionFactory.createTransaction(false)) {

            try (PreparedStatement positionUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_positions\" (ulid, ordering_group, sequence_number, position, ts) VALUES (?, ?, ?, ?, ?)", topic))) {

                try (PreparedStatement contentUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_content\" (ulid, name, data) VALUES (?, ?, ?)", topic))) {

                    for (RawdataMessage message : messages) {

                        UUID id = getOrGenerateNextUlid(message);
                        UUID uuid = id;

                        /*
                         * position
                         */
                        positionUpdate.setObject(1, uuid);
                        positionUpdate.setString(2, message.orderingGroup());
                        positionUpdate.setLong(3, message.sequenceNumber());
                        positionUpdate.setString(4, message.position());
                        positionUpdate.setTimestamp(5, Timestamp.from(new Date(RawdataIds.timestamp(id)).toInstant()));
                        positionUpdate.addBatch();

                        /*
                         * content
                         */

                        for (Map.Entry<String, byte[]> entry : message.data().entrySet()) {
                            contentUpdate.setObject(1, uuid);
                            contentUpdate.setString(2, entry.getKey());
                            contentUpdate.setBytes(3, entry.getValue());
                            contentUpdate.addBatch();
                        }
                    }

                    positionUpdate.executeBatch();

                    contentUpdate.executeBatch();
                }
            }

        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    private UUID getOrGenerateNextUlid(RawdataMessage message) {
        UUID id = message.id();
        while (id == null) {
            UUID previousUlid = previousIdRef.get();
            UUID attemptedId = RawdataIds.next(previousUlid);
            if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                id = attemptedId;
            }
        }
        return id;
    }

    @Override
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        return CompletableFuture.runAsync(() -> publish(messages));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
