package io.descoped.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;
import io.descoped.rawdata.api.RawdataMessage;

import java.time.Duration;

public class DiscardingRawdataClient implements RawdataClient {

    @Override
    public DiscardingRawdataProducer producer(String topic) {
        return new DiscardingRawdataProducer(topic);
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        return new DiscardingRawdataConsumer(topic);
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return null;
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        return null;
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public DiscardingRawdataMetadataClient metadata(String topic) {
        return new DiscardingRawdataMetadataClient(topic);
    }
}
