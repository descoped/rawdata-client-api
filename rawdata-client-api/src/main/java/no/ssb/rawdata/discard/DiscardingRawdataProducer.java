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
    public void publish(RawdataMessage... messages) {
    }

    static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
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
