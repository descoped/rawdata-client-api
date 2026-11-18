package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.PollingRawdataConsumer;

import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Reads a topic segment by segment and polls for the next; a publish through the same client wakes it at once. A
 * start position (a cursor or a seek) is a lower bound on the ids: the consumer finds the first segment that reaches
 * it and skips the messages before it.
 */
final class AvroRawdataConsumer extends PollingRawdataConsumer {

    private final AvroRawdataClient client;
    private long next = -1; // guarded by the turn: the segment to read next; -1 until the start is found
    private UUID from; // guarded by the turn: the lower bound on ids, null once passed
    private boolean fromInclusive; // guarded by the turn

    AvroRawdataConsumer(AvroRawdataClient client, String topic, AvroCursor start, long pollIntervalMillis) {
        super(topic, pollIntervalMillis);
        this.client = client;
        if (start == null) {
            next = 0;
        } else {
            from = start.id;
            fromInclusive = start.inclusive;
        }
    }

    @Override
    protected void fill(Deque<RawdataMessage> buffer) {
        if (next < 0) {
            next = client.firstSegmentReaching(topic(), from);
        }
        while (buffer.isEmpty()) {
            List<RawdataMessage> messages = client.segment(topic(), next);
            if (messages == null) {
                return;
            }
            next++;
            for (RawdataMessage message : messages) {
                if (from == null || passes(message.id())) {
                    buffer.addLast(message);
                    from = null; // ids only grow from here
                }
            }
        }
    }

    private boolean passes(UUID id) {
        int order = RawdataIds.ORDER.compare(id, from);
        return order > 0 || order == 0 && fromInclusive;
    }

    @Override
    protected void reposition(long timestamp) {
        next = -1;
        from = RawdataIds.beginningOf(timestamp);
        fromInclusive = true;
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
