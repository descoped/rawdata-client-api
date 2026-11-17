package io.descoped.rawdata.api;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Copies messages from a consumer to a producer, for example from a topic in one provider to a topic in another.
 * Messages keep their ids, so the copy has the same order and the same ids as the source. The copy is pulled in
 * batches of a size the caller chooses, so memory use is bounded by one batch.
 * <pre>{@code
 * try (RawdataConsumer from = source.consumer("topic"); RawdataProducer to = target.producer("topic")) {
 *     long copied = RawdataCopy.copy(from, to, 500, Duration.ofSeconds(1));
 * }
 * }</pre>
 */
public final class RawdataCopy {

    private RawdataCopy() {
    }

    /**
     * Copies until the source has no message for the idle timeout, publishing each batch in one call. Neither the
     * consumer nor the producer is closed. If a publish fails, the batch that failed is not copied, and the exception
     * is thrown; the consumer's position is then after that batch.
     *
     * @param from        the consumer to read
     * @param to          the producer to write
     * @param batchSize   the most messages per publish, 1 or more
     * @param idleTimeout how long to wait for a message before the copy is done
     * @return the number of messages copied
     * @throws IllegalArgumentException if the batch size is less than 1 or the timeout is negative
     * @throws InterruptedException     if the thread is interrupted while waiting
     * @throws RawdataClosedException   if the consumer or the producer is closed
     */
    public static long copy(RawdataConsumer from, RawdataProducer to, int batchSize, Duration idleTimeout) throws InterruptedException {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        long copied = 0;
        for (; ; ) {
            List<RawdataMessage> batch = from.receiveUpTo(batchSize, idleTimeout);
            if (batch.isEmpty()) {
                return copied;
            }
            to.publish(batch);
            copied += batch.size();
        }
    }
}
