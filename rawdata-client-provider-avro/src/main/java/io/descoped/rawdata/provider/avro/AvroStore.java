package io.descoped.rawdata.provider.avro;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Where an {@link AvroRawdataClient} keeps its topics: a folder, a bucket. A topic is a sequence of segments numbered
 * 0, 1, 2 and so on, each an Avro file holding the messages of one upload window, and a set of metadata entries.
 * <p>
 * Segments are large (up to {@code avro-file.max.bytes}), so they are read in ranges through {@link #open}: a seek
 * reads only a segment's header, {@code lastMessage} only its last block, and a consumer streams it.
 * <p>
 * The client relies on two properties of {@link #create}: a segment is created only if no segment with its number
 * exists, and readers see the whole segment or nothing. Writers of a topic, in this process or another, therefore
 * add segments one at a time, each after the last, and the segments of a topic are numbered without gaps.
 * <p>
 * Topic names reach the store already validated ({@link io.descoped.rawdata.api.RawdataTopics}), and are distinct by
 * case; a store on a case-insensitive file system must keep {@code Foo} and {@code foo} apart. Metadata names are
 * {@code k} followed by lowercase hexadecimal.
 */
public interface AvroStore extends AutoCloseable {

    /**
     * Creates a segment, unless a segment with the number exists.
     *
     * @param topic   the topic
     * @param segment the segment number, 0 or more
     * @param content the Avro file
     * @return true if the segment was created; false if it existed, in which case nothing was written
     * @throws IOException if the store fails
     */
    boolean create(String topic, long segment, byte[] content) throws IOException;

    /**
     * Opens a segment for reading in ranges. The segment is pinned to the object found now: its reads return that
     * object's bytes, or fail, never another object's. It holds no open resources, so it needs no closing.
     *
     * @param topic   the topic
     * @param segment the segment number
     * @return the segment, or empty if there is no such segment
     * @throws IOException if the store fails
     */
    Optional<Segment> open(String topic, long segment) throws IOException;

    /**
     * Reads a whole segment.
     *
     * @param topic   the topic
     * @param segment the segment number
     * @return the Avro file, or empty if there is no such segment
     * @throws IOException if the store fails
     */
    default Optional<byte[]> read(String topic, long segment) throws IOException {
        Optional<Segment> opened = open(topic, segment);
        if (opened.isEmpty()) {
            return Optional.empty();
        }
        Segment found = opened.get();
        return Optional.of(found.read(0, Math.toIntExact(found.size())));
    }

    /**
     * A segment opened for reading: its size, and its bytes in ranges.
     */
    interface Segment {

        /**
         * The segment's size.
         *
         * @return the size in bytes
         */
        long size();

        /**
         * Reads a range of the segment.
         *
         * @param offset where the range starts, at most {@link #size()}
         * @param length how many bytes to read; fewer are returned only where the segment ends
         * @return the bytes
         * @throws IOException if the store fails, or the segment is no longer the object that was opened
         */
        byte[] read(long offset, int length) throws IOException;
    }

    /**
     * Whether a segment exists.
     *
     * @param topic   the topic
     * @param segment the segment number
     * @return true if it exists
     * @throws IOException if the store fails
     */
    boolean exists(String topic, long segment) throws IOException;

    /**
     * Lists the names of a topic's metadata entries.
     *
     * @param topic the topic
     * @return the names, empty for a topic without metadata
     * @throws IOException if the store fails
     */
    Set<String> metadataNames(String topic) throws IOException;

    /**
     * Reads a metadata entry.
     *
     * @param topic the topic
     * @param name  the entry's name
     * @return the value, or empty if there is no such entry
     * @throws IOException if the store fails
     */
    Optional<byte[]> readMetadata(String topic, String name) throws IOException;

    /**
     * Writes a metadata entry, replacing any value it had; readers see the old value or the new, never a mix.
     *
     * @param topic the topic
     * @param name  the entry's name
     * @param value the value
     * @throws IOException              if the store fails
     * @throws IllegalArgumentException if the name is too long for the store
     */
    void writeMetadata(String topic, String name, byte[] value) throws IOException;

    /**
     * Removes a metadata entry; does nothing if there is none.
     *
     * @param topic the topic
     * @param name  the entry's name
     * @throws IOException if the store fails
     */
    void deleteMetadata(String topic, String name) throws IOException;

    /**
     * Registers what the client does when the store is throttled: a response that asks it to slow down (GCS 429 or
     * 503, S3 503 SlowDown) or a request that timed out, on any attempt, also one the store retries and then succeeds.
     * The client lowers how many uploads run at once ({@link AvroUploadLimiter}). A store that cannot tell does
     * nothing.
     *
     * @param listener called on each throttled response, from the store's threads
     */
    default void onThrottled(Runnable listener) {
        // a store that cannot tell
    }

    /**
     * Releases what the store holds, for example a connection pool. Called once, when the client closes.
     */
    @Override
    void close();
}
