package no.ssb.rawdata.api.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface RawdataConsumer {

    RawdataMessage receive(Long timeout, TimeUnit timeUnit) throws InterruptedException;

    CompletableFuture<RawdataMessage> receiveAsync();

    void acknowledge(RawdataMessageId id);

    void acknowledgeAccumulative(RawdataMessageId id);

}
