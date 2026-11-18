package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.PollingRawdataConsumer;

import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Reads a topic segment by segment and polls for the next; an upload through the same client wakes it at once. A
 * segment is streamed in ranges, so only one range of it is in memory, and at most {@value #BATCH} messages are
 * buffered at a time. A start position (a cursor or a seek) is a lower bound on the ids: the consumer finds the first
 * segment that reaches it, reading only segment headers, and skips the messages before it.
 */
final class AvroRawdataConsumer extends PollingRawdataConsumer {

    static final int BATCH = 1000;

    private final AvroRawdataClient client;
    private AvroSegmentReader reader; // guarded by the turn: the segment being read, null between segments
    private UUID delivered; // guarded by the turn: the id of the last message put in the buffer
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
            if (reader == null) {
                reader = client.reader(topic(), next);
                if (reader == null) {
                    return; // not uploaded yet
                }
            }
            for (int read = 0; read < BATCH; read++) {
                RawdataMessage message = nextOrResume();
                if (message == null) {
                    reader = null; // the segment is read: nothing to release, the next one is opened when wanted
                    next++;
                    break;
                }
                if (from == null || passes(message.id())) {
                    buffer.addLast(message);
                    delivered = message.id();
                    from = null; // ids only grow from here
                }
            }
        }
    }

    /**
     * The next message of the segment. If reading fails (a store error), the reader is dropped and the next fill opens
     * the segment again and skips what was delivered: a stream that failed halfway through a block cannot go on.
     */
    private RawdataMessage nextOrResume() {
        try {
            return client.next(reader, topic(), next);
        } catch (RuntimeException e) {
            reader = null;
            if (delivered != null) {
                from = delivered;
                fromInclusive = false;
            }
            throw e;
        }
    }

    private boolean passes(UUID id) {
        int order = RawdataIds.ORDER.compare(id, from);
        return order > 0 || order == 0 && fromInclusive;
    }

    @Override
    protected void reposition(long timestamp) {
        reader = null;
        delivered = null;
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
