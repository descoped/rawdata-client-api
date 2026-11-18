package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.OrderedRawdataProducer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes a call's messages as one segment, the one after the topic's last. Ids are given after the last id of
 * the last segment, and given ids are checked against it. If another writer creates that segment first, the
 * producer reads the new last segment and tries again, so a call is written whole after everything before it, or
 * not at all.
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
            // The last segment this client saw; if it is no longer the last, creating the next fails and says so.
            AvroRawdataClient.Tail tail = client.seenTail(topic);
            for (; ; ) {
                checkOpen();
                List<RawdataMessage> stored = withIds(batch, tail.lastId());
                long segment = tail.segment() + 1;
                if (client.store().create(topic, segment, AvroSegments.encode(stored))) {
                    client.published(topic, new AvroRawdataClient.Tail(segment, stored.get(stored.size() - 1).id()));
                    return;
                }
                tail = client.tail(topic); // another writer was first
            }
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
