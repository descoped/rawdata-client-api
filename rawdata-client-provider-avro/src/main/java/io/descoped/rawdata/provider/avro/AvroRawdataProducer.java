package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataWriterConflictException;
import io.descoped.rawdata.api.spi.OrderedRawdataProducer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes a call's messages as one segment, the one after the last this client wrote (or found, before its first
 * publish). Ids are given after the last id of that segment, and given ids are checked against it. A call is written
 * whole, or not at all; if the segment exists already, another process writes the topic, and the publish fails with
 * {@link RawdataWriterConflictException}.
 */
final class AvroRawdataProducer extends OrderedRawdataProducer {

    private final AvroRawdataClient client;

    AvroRawdataProducer(AvroRawdataClient client, String topic) {
        super(topic);
        this.client = client;
    }

    @Override
    protected void write(RawdataMessage[] batch) {
        String topic = topic();
        // Producers of this client take turns, so they do not race each other for the next segment.
        ReentrantLock lock = client.publishLock(topic);
        lock.lock();
        try {
            AvroRawdataClient.Tail tail = client.writerTail(topic);
            checkOpen();
            List<RawdataMessage> stored = withIds(batch, tail.lastId());
            long segment = tail.segment() + 1;
            if (!client.store().create(topic, segment, AvroSegments.encode(stored))) {
                throw new RawdataWriterConflictException(topic); // another process created the segment
            }
            client.published(topic, new AvroRawdataClient.Tail(segment, stored.get(stored.size() - 1).id()));
        } catch (IOException e) {
            throw client.failure("Publishing to topic " + topic, e);
        } finally {
            lock.unlock();
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
