package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.PollingRawdataConsumer;

import java.util.Deque;
import java.util.concurrent.Executor;

/**
 * Reads a topic in batches of up to the prefetch size, and polls it at the poll interval when it has nothing new.
 */
final class PostgresRawdataConsumer extends PollingRawdataConsumer {

    private final PostgresRawdataClient client;
    private final int prefetchSize;
    private PostgresCursor position; // guarded by the turn: where the next fetch starts

    PostgresRawdataConsumer(PostgresRawdataClient client, String topic, PostgresCursor start, int prefetchSize, long pollIntervalMillis) {
        super(topic, pollIntervalMillis);
        this.client = client;
        this.prefetchSize = prefetchSize;
        this.position = start != null ? start : new PostgresCursor(RawdataIds.beginningOfTime(), true);
    }

    @Override
    protected void fill(Deque<RawdataMessage> buffer) {
        PostgresCursor from = position;
        buffer.addAll(client.jdbc().transaction(connection ->
                PostgresMessages.after(connection, topic(), from.id, from.inclusive, prefetchSize)));
        if (!buffer.isEmpty()) {
            position = new PostgresCursor(buffer.peekLast().id(), false);
        }
    }

    @Override
    protected void reposition(long timestamp) {
        position = new PostgresCursor(RawdataIds.beginningOf(timestamp), true);
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
