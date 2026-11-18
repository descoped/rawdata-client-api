package io.descoped.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariDataSource;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@code postgres} provider's client. It owns its connection pool and closes it on {@link #close()}.
 */
final class PostgresRawdataClient implements RawdataClient {

    private static final AtomicInteger THREADS = new AtomicInteger();

    private final HikariDataSource dataSource;
    private final Jdbc jdbc;
    private final int prefetchSize;
    private final long pollIntervalMillis;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Object> open = ConcurrentHashMap.newKeySet(); // producers and consumers
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "rawdata-postgres-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    PostgresRawdataClient(HikariDataSource dataSource, int prefetchSize, long pollIntervalMillis) {
        this.dataSource = dataSource;
        this.jdbc = new Jdbc(dataSource);
        this.prefetchSize = prefetchSize;
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public RawdataProducer producer(String topic) {
        PostgresRawdataProducer producer = new PostgresRawdataProducer(this, checked(topic));
        open.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        String name = checked(topic);
        if (cursor != null && !(cursor instanceof PostgresCursor)) {
            throw new IllegalArgumentException("Not a cursor of the postgres provider: " + cursor.getClass().getName());
        }
        PostgresRawdataConsumer consumer = new PostgresRawdataConsumer(this, name, (PostgresCursor) cursor, prefetchSize, pollIntervalMillis);
        open.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, UUID id, boolean inclusive) {
        checked(topic);
        return new PostgresCursor(RawdataIds.requireValid(id), inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        String name = checked(topic);
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(tolerance, "tolerance");
        UUID lowerBound = RawdataIds.beginningOf(approxTimestamp - tolerance.toMillis());
        UUID upperBound = RawdataIds.beginningOf(approxTimestamp + tolerance.toMillis() + 1);
        UUID id = jdbc.transaction(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT id FROM rawdata_message WHERE topic = ? AND position = ? AND id >= ? AND id < ? ORDER BY id LIMIT 1")) {
                select.setString(1, name);
                select.setString(2, position);
                select.setObject(3, lowerBound);
                select.setObject(4, upperBound);
                try (ResultSet rows = select.executeQuery()) {
                    return rows.next() ? rows.getObject(1, UUID.class) : null;
                }
            }
        });
        if (id == null) {
            throw new RawdataNoSuchPositionException("Position not found in topic " + name + ": " + position);
        }
        return new PostgresCursor(id, inclusive);
    }

    @Override
    public RawdataMessage lastMessage(String topic) {
        String name = checked(topic);
        return jdbc.transaction(connection -> PostgresMessages.last(connection, name));
    }

    @Override
    public RawdataMetadataClient metadata(String topic) {
        return new PostgresRawdataMetadataClient(this, checked(topic));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Object producerOrConsumer : open) {
            if (producerOrConsumer instanceof PostgresRawdataProducer producer) {
                producer.close();
            } else if (producerOrConsumer instanceof PostgresRawdataConsumer consumer) {
                consumer.close();
            }
        }
        executor.shutdownNow();
        dataSource.close();
    }

    Jdbc jdbc() {
        return jdbc;
    }

    ExecutorService executor() {
        return executor;
    }

    void removed(Object producerOrConsumer) {
        open.remove(producerOrConsumer);
    }

    void checkOpen() {
        if (isClosed()) {
            throw new RawdataClosedException("client is closed");
        }
    }

    private String checked(String topic) {
        RawdataTopics.requireValid(topic);
        checkOpen();
        return topic;
    }
}
