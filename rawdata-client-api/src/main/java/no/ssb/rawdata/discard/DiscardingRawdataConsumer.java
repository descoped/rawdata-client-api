package no.ssb.rawdata.discard;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class DiscardingRawdataConsumer implements RawdataConsumer {

    final String topic;

    DiscardingRawdataConsumer(String topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws RawdataClosedException {
        return null;
    }

    static final CompletableFuture<RawdataMessage> COMPLETED = CompletableFuture.completedFuture(null);

    @Override
    public CompletableFuture<RawdataMessage> receiveAsync() {
        return COMPLETED;
    }

    @Override
    public void seek(long timestamp) {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }
}
