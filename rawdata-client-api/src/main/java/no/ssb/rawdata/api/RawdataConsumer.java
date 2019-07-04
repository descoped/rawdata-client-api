package no.ssb.rawdata.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface RawdataConsumer {

    RawdataMessage receive(long timeout, TimeUnit timeUnit) throws InterruptedException;

    CompletableFuture<? extends RawdataMessage> receiveAsync();

    void acknowledgeAccumulative(RawdataMessageId id);

}
