package no.ssb.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;

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
}
