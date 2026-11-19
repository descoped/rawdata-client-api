package io.descoped.rawdata.api;

import io.descoped.service.provider.api.ProviderConfigurator;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A client for one rawdata store: creates producers and consumers on topics, finds positions, and reads and writes
 * topic metadata.
 * <p>
 * The contract every provider keeps:
 * <ul>
 *     <li>A topic name follows {@link RawdataTopics}; a method given an invalid name throws
 *     {@link IllegalArgumentException}. A topic exists from its first use; an unknown topic is empty.</li>
 *     <li>Ids are UUIDv7s, ordered by {@link RawdataIds#ORDER}. An id the producer assigns is greater than every id
 *     already in the topic, whichever producer wrote it. An id the caller gives is kept as it is, in any order. A topic
 *     whose ids were published in increasing order is consumed in that order; a lower id published later is read
 *     where the provider puts it, in id order or after what was published before it. A consumer of a topic that is
 *     still written may miss a message whose id lands behind its position; a consumer of a completed topic, read from
 *     the beginning, gets every message once.</li>
 *     <li>After {@link #close()}, every method of the client and of the producers, consumers and metadata clients it
 *     created throws {@link RawdataClosedException}, except {@code isClosed()} and {@code close()}, which may be
 *     called any number of times.</li>
 *     <li>Messages and metadata values are copied in and out; nothing a caller holds is shared with the store.</li>
 * </ul>
 */
public interface RawdataClient extends AutoCloseable {

    /**
     * The configuration key that names the provider, e.g. {@code memory} or {@code postgres}.
     */
    String PROVIDER_KEY = "rawdata.client.provider";

    /**
     * Creates a client from the provider named by {@link #PROVIDER_KEY} in the configuration. The whole configuration
     * is passed to the provider.
     *
     * @param configuration the configuration
     * @return a new client
     * @throws IllegalArgumentException if the configuration does not name a provider
     * @throws io.descoped.service.provider.api.ProviderException if no provider by that name is available
     */
    static RawdataClient create(Map<String, String> configuration) {
        Objects.requireNonNull(configuration, "configuration");
        String provider = configuration.get(PROVIDER_KEY);
        if (provider == null || provider.isBlank()) {
            throw new IllegalArgumentException("Missing configuration '" + PROVIDER_KEY + "'");
        }
        return ProviderConfigurator.configure(RawdataClientInitializer.class, provider, configuration);
    }

    /**
     * Creates a producer on a topic.
     *
     * @param topic the topic
     * @return a producer on the topic
     */
    RawdataProducer producer(String topic);

    /**
     * Creates a consumer that reads the topic from the beginning.
     *
     * @param topic the topic
     * @return a consumer on the topic
     */
    default RawdataConsumer consumer(String topic) {
        return consumer(topic, (RawdataCursor) null);
    }

    /**
     * Creates a consumer that reads the topic from a cursor.
     *
     * @param topic  the topic
     * @param cursor where to start, from {@link #cursorOf}; null for the beginning
     * @return a consumer on the topic
     * @throws IllegalArgumentException if the cursor was not created by this client's provider
     */
    RawdataConsumer consumer(String topic, RawdataCursor cursor);

    /**
     * Creates a consumer that reads the topic from the message after an id.
     *
     * @param topic           the topic
     * @param initialPosition the id to start after; null for the beginning
     * @return a consumer on the topic
     */
    default RawdataConsumer consumer(String topic, UUID initialPosition) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, false));
    }

    /**
     * Creates a consumer that reads the topic from an id.
     *
     * @param topic           the topic
     * @param initialPosition the id to start at; null for the beginning
     * @param inclusive       whether the message with that id is read first
     * @return a consumer on the topic
     */
    default RawdataConsumer consumer(String topic, UUID initialPosition, boolean inclusive) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, inclusive));
    }

    /**
     * Creates a consumer that reads the topic from the message after the first message with a position.
     *
     * @param topic           the topic
     * @param initialPosition the position to start after; null for the beginning
     * @param approxTimestamp when the message was written, approximately, in milliseconds since 1970-01-01T00:00Z
     * @param tolerance       the search range: {@code [approxTimestamp - tolerance, approxTimestamp + tolerance]}
     * @return a consumer on the topic
     * @throws RawdataNoSuchPositionException if the position is not found in the range
     */
    default RawdataConsumer consumer(String topic, String initialPosition, long approxTimestamp, Duration tolerance) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, false, approxTimestamp, tolerance));
    }

    /**
     * Creates a consumer that reads the topic from the first message with a position.
     *
     * @param topic           the topic
     * @param initialPosition the position to start at; null for the beginning
     * @param inclusive       whether the message with that position is read first
     * @param approxTimestamp when the message was written, approximately, in milliseconds since 1970-01-01T00:00Z
     * @param tolerance       the search range: {@code [approxTimestamp - tolerance, approxTimestamp + tolerance]}
     * @return a consumer on the topic
     * @throws RawdataNoSuchPositionException if the position is not found in the range
     */
    default RawdataConsumer consumer(String topic, String initialPosition, boolean inclusive, long approxTimestamp, Duration tolerance) {
        return consumer(topic, initialPosition == null ? null : cursorOf(topic, initialPosition, inclusive, approxTimestamp, tolerance));
    }

    /**
     * Creates a cursor at an id. The id need not be the id of a message: reading starts at the first message with a
     * greater id, or at an equal one if inclusive. {@link RawdataIds#beginningOf(long)} gives a cursor at a time.
     *
     * @param topic     the topic
     * @param id        the id, a UUIDv7
     * @param inclusive whether a message with exactly that id is read first
     * @return the cursor
     * @throws IllegalArgumentException if the id is not a UUIDv7
     */
    RawdataCursor cursorOf(String topic, UUID id, boolean inclusive);

    /**
     * Finds the first message with a position, searching the messages written in
     * {@code [approxTimestamp - tolerance, approxTimestamp + tolerance]}, and creates a cursor at it.
     *
     * @param topic           the topic
     * @param position        the position
     * @param inclusive       whether that message is read first
     * @param approxTimestamp when the message was written, approximately, in milliseconds since 1970-01-01T00:00Z
     * @param tolerance       how far from the timestamp to search
     * @return the cursor, never null
     * @throws RawdataNoSuchPositionException if no message with the position is found in the range
     */
    RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException;

    /**
     * Reads the last message of a topic: the last one a consumer reading the whole topic gets. With ids published in
     * increasing order, the one with the greatest id.
     *
     * @param topic the topic
     * @return the last message, or null if the topic is empty
     */
    RawdataMessage lastMessage(String topic);

    /**
     * Whether the client is closed.
     *
     * @return true after {@link #close()}
     */
    boolean isClosed();

    /**
     * Closes the client and every producer, consumer and metadata client it created. Calling it again does nothing.
     */
    @Override
    void close();

    /**
     * Returns the metadata client of a topic: a small key-value store that belongs to the topic and lasts as long as
     * the topic's messages do.
     *
     * @param topic the topic
     * @return the metadata client of the topic
     */
    RawdataMetadataClient metadata(String topic);
}
