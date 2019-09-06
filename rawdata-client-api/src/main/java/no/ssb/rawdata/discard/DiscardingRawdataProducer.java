package no.ssb.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.Collections;
import java.util.Set;
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
            public RawdataMessage.Builder orderingGroup(String group) {
                return this;
            }

            @Override
            public String orderingGroup() {
                return null;
            }

            @Override
            public RawdataMessage.Builder sequenceNumber(long sequenceNumber) {
                return this;
            }

            @Override
            public long sequenceNumber() {
                return 0;
            }

            @Override
            public RawdataMessage.Builder ulid(ULID.Value ulid) {
                return this;
            }

            @Override
            public ULID.Value ulid() {
                return null;
            }

            @Override
            public RawdataMessage.Builder position(String position) {
                return this;
            }

            @Override
            public String position() {
                return null;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                return this;
            }

            @Override
            public Set<String> keys() {
                return Collections.emptySet();
            }

            @Override
            public byte[] get(String key) {
                return new byte[0];
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
