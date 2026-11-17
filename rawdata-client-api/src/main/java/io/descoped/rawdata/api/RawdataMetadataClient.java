package io.descoped.rawdata.api;

import java.util.Set;

/**
 * Reads and writes the metadata of one topic: entries of a key and bytes, for example a checkpoint. The entries
 * belong to the topic, so every metadata client of the topic sees the same entries.
 */
public interface RawdataMetadataClient {

    /**
     * Returns the topic this metadata client belongs to.
     *
     * @return the topic
     */
    String topic();

    /**
     * Returns the keys of the topic's metadata entries.
     *
     * @return an unmodifiable snapshot of the keys
     * @throws RawdataClosedException if the client is closed
     */
    Set<String> keys();

    /**
     * Reads an entry.
     *
     * @param key the key
     * @return a copy of the value, or null if there is no entry with the key
     * @throws RawdataClosedException if the client is closed
     */
    byte[] get(String key);

    /**
     * Writes an entry, replacing any value the key had. The store keeps a copy of the array.
     *
     * @param key   the key
     * @param value the value
     * @return this metadata client
     * @throws NullPointerException   if the key or the value is null
     * @throws RawdataClosedException if the client is closed
     */
    RawdataMetadataClient put(String key, byte[] value);

    /**
     * Removes an entry; does nothing if there is none.
     *
     * @param key the key
     * @return this metadata client
     * @throws RawdataClosedException if the client is closed
     */
    RawdataMetadataClient remove(String key);
}
