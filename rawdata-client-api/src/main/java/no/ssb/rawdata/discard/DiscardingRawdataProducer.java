package no.ssb.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.concurrent.CompletableFuture;

class DiscardingRawdataProducer implements RawdataProducer {

    final String topic;

    DiscardingRawdataProducer(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage.Builder builder() {
        return new RawdataMessage.Builder() {
            @Override
            public RawdataMessage.Builder ulid(ULID.Value ulid) {
                return this;
            }

            @Override
            public RawdataMessage.Builder position(String position) {
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                return this;
            }

            @Override
            public RawdataMessage build() {
                return null;
            }
        };
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder _builder) {
        return this;
    }

    @Override
    public void publish(String... positions) {
    }

    static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        return COMPLETED;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }
}
