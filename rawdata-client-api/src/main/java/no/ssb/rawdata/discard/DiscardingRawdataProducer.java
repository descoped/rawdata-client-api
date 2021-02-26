package no.ssb.rawdata.discard;

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
        return RawdataMessage.builder();
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
