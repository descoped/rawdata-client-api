package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.PollingRawdataConsumer;

import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Reads a topic segment by segment, each in the order its messages were published, and polls for the next; an upload
 * through the same client wakes it at once. A segment is streamed in ranges, so only one range of it is in memory, and
 * at most {@value #BATCH} messages are buffered at a time.
 * <p>
 * Ids may be given in any order, so segments may overlap in ids. A start from a cursor works as 2.x did: the consumer
 * finds the message with the cursor's id, reading segment headers to skip the segments that cannot hold it, and reads
 * everything after it in segment order, also a lower id published later. If no uploaded message has the id, and on a
 * seek, it starts at the first message whose id is at or past the bound, from the first segment that reaches it.
 */
final class AvroRawdataConsumer extends PollingRawdataConsumer {

    static final int BATCH = 1000;

    private final AvroRawdataClient client;
    private AvroSegmentReader reader; // guarded by the turn: the segment being read, null between segments
    private long next = -1; // guarded by the turn: the segment to read next; -1 until the start is found
    private long read; // guarded by the turn: how many messages of the open segment were read
    private long skip; // guarded by the turn: how many to skip when the segment is opened again after a failure
    private UUID from; // guarded by the turn: where to start, null once started
    private boolean fromInclusive; // guarded by the turn
    private boolean exact; // guarded by the turn: start at the message with the id `from`, found in segment `next`

    AvroRawdataConsumer(AvroRawdataClient client, String topic, AvroCursor start, long pollIntervalMillis) {
        super(topic, pollIntervalMillis);
        this.client = client;
        if (start == null) {
            next = 0;
        } else {
            from = start.id;
            fromInclusive = start.inclusive;
            exact = true;
        }
    }

    @Override
    protected void fill(Deque<RawdataMessage> buffer) {
        if (next < 0) {
            next = exact ? client.segmentThatMayHold(topic(), from, 0) : -1;
            if (next < 0) {
                exact = false;
                next = client.firstSegmentReaching(topic(), from);
            }
        }
        while (buffer.isEmpty()) {
            if (reader == null) {
                reader = client.reader(topic(), next);
                if (reader == null) {
                    return; // not uploaded yet
                }
            }
            readBatch(buffer);
        }
    }

    /**
     * Reads up to {@value #BATCH} messages of the open segment into the buffer, skipping those before the start.
     */
    private void readBatch(Deque<RawdataMessage> buffer) {
        for (int i = 0; i < BATCH; i++) {
            RawdataMessage message = nextOrResume();
            if (message == null) {
                reader = null; // the segment is read: nothing to release, the next one is opened when wanted
                read = 0;
                if (exact && from != null) {
                    lookFurther();
                } else {
                    next++;
                }
                return;
            }
            read++;
            if (skip > 0) {
                skip--; // delivered before the segment was opened again
            } else if (from == null) {
                buffer.addLast(message);
            } else if (exact ? message.id().equals(from) : passes(message.id())) {
                if (!exact || fromInclusive) {
                    buffer.addLast(message);
                }
                from = null; // from here on, everything in segment order
            }
        }
    }

    /**
     * The next message of the segment. If reading fails (a store error), the reader is dropped and the next fill opens
     * the segment again and skips what was read: a stream that failed halfway through a block cannot go on.
     */
    private RawdataMessage nextOrResume() {
        try {
            return client.next(reader, topic(), next);
        } catch (RuntimeException e) {
            reader = null;
            skip = read;
            read = 0;
            throw e;
        }
    }

    /**
     * The cursor's message was not in this segment: on to the next one that may hold it, by its header. If none does,
     * the consumer starts as a seek does, at the first message at or past the id.
     */
    private void lookFurther() {
        long further = client.segmentThatMayHold(topic(), from, next + 1);
        if (further >= 0) {
            next = further;
        } else {
            exact = false;
            next = client.firstSegmentReaching(topic(), from);
        }
    }

    private boolean passes(UUID id) {
        int order = RawdataIds.ORDER.compare(id, from);
        return order > 0 || order == 0 && fromInclusive;
    }

    @Override
    protected void reposition(long timestamp) {
        reader = null;
        next = -1;
        read = 0;
        skip = 0;
        from = RawdataIds.beginningOf(timestamp);
        fromInclusive = true;
        exact = false;
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
