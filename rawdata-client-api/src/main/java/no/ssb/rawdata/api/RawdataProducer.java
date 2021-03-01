package no.ssb.rawdata.api;

import de.huxhorn.sulky.ulid.ULID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RawdataProducer extends AutoCloseable {

    Logger LOG = LoggerFactory.getLogger(RawdataProducer.class);

    /**
     * @return the topic on which this producer will publish messages.
     */
    String topic();

    /**
     * Publish messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @throws RawdataClosedException if the producer was closed before or during this call.
     */
    default void publish(List<RawdataMessage> messages) throws RawdataClosedException {
        publish(messages.toArray(new RawdataMessage[0]));
    }

    /**
     * Publish messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @throws RawdataClosedException if the producer was closed before or during this call.
     */
    void publish(RawdataMessage... messages) throws RawdataClosedException;

    /**
     * Asynchronously publish all messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @return a completable futures representing the completeness of the async-function.
     */
    default CompletableFuture<Void> publishAsync(List<RawdataMessage> messages) {
        return publishAsync(messages.toArray(new RawdataMessage[messages.size()]));
    }

    /**
     * Asynchronously publish all messages. Published content will be assigned an ulid if it is missing from any of the provided messages.
     *
     * @param messages a list of messages
     * @return a completable futures representing the completeness of the async-function.
     */
    CompletableFuture<Void> publishAsync(RawdataMessage... messages);

    /**
     * Returns whether or not the producer is closed.
     *
     * @return whether the producer is closed.
     */
    boolean isClosed();

    @Override
    void close();

    /**
     * Generate a new unique ulid. If the newly generated ulid has a new timestamp than the previous one, then the very
     * least significant bit will be set to 1 (which is higher than beginning-of-time ulid used by consumer).
     *
     * @param generator    the ulid generator
     * @param previousUlid the previous ulid in the sequence
     * @return the generated ulid
     */
    static ULID.Value nextMonotonicUlid(ULID generator, ULID.Value previousUlid) {
        /*
         * Will spin until time ticks if next value overflows.
         * Although theoretically possible, it is extremely unlikely that the loop will ever spin
         */
        ULID.Value value;
        do {
            long timestamp = System.currentTimeMillis();
            long diff = timestamp - previousUlid.timestamp();
            if (diff < 0) {
                if (diff < -(30 * 1000)) {
                    throw new IllegalStateException(String.format("Previous timestamp is in the future. Diff %d ms", -diff));
                }
                LOG.debug("Previous timestamp is in the future, waiting for time to catch up. Diff {} ms", -diff);
                try {
                    Thread.sleep(-diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else if (diff > 0) {
                // start at lsb 1, to avoid inclusive/exclusive semantics when searching
                return new ULID.Value((timestamp << 16) & 0xFFFFFFFFFFFF0000L, 1L);
            }
            // diff == 0
            value = generator.nextStrictlyMonotonicValue(previousUlid, timestamp).orElse(null);
        } while (value == null);
        return value;
    }
}
