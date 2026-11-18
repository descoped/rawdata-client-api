package io.descoped.rawdata.provider.avro;

import java.io.IOException;
import java.util.Set;

/**
 * Where an {@link AvroRawdataClient} keeps its topics: a folder, a bucket. A topic is a sequence of segments numbered
 * 0, 1, 2 and so on, each an Avro file holding the messages of one publish, and a set of metadata entries.
 * <p>
 * The client relies on two properties of {@link #create}: a segment is created only if no segment with its number
 * exists, and readers see the whole segment or nothing. Writers of a topic, in this process or another, therefore
 * add segments one at a time, each after the last, and the segments of a topic are numbered without gaps.
 * <p>
 * Topic names reach the store already validated ({@link io.descoped.rawdata.api.RawdataTopics}); metadata names are
 * {@code k} followed by lowercase hexadecimal. Both are safe as file and object names.
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
     * Reads a segment.
     *
     * @param topic   the topic
     * @param segment the segment number
     * @return the Avro file, or null if there is no such segment
     * @throws IOException if the store fails
     */
    byte[] read(String topic, long segment) throws IOException;

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
     * @return the value, or null if there is no such entry
     * @throws IOException if the store fails
     */
    byte[] readMetadata(String topic, String name) throws IOException;

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
     * Releases what the store holds, for example a connection pool. Called once, when the client closes.
     */
    @Override
    void close();
}
