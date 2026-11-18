package io.descoped.rawdata.api;

import java.util.Set;

/**
 * Reads and writes the metadata of one topic: entries of a key and bytes, for example a checkpoint. The entries
 * belong to the topic, so every metadata client of the topic sees the same entries.
 * <p>
 * A key is any string without the NUL character (U+0000), the empty string included; the methods that take
 * a key check it with {@link #requireValidKey(String)}. How long a key may be depends on the store, and each provider
 * documents its limit; a longer key is refused with {@link IllegalArgumentException}. Every provider takes keys of
 * at least {@value #PORTABLE_KEY_BYTES} bytes in UTF-8.
 */
public interface RawdataMetadataClient {

    /**
     * The key length, in UTF-8 bytes, that every provider takes.
     */
    int PORTABLE_KEY_BYTES = 127;

    /**
     * Checks a metadata key: not null, and no NUL character.
     *
     * @param key the key
     * @return the key, unchanged
     * @throws NullPointerException     if the key is null
     * @throws IllegalArgumentException if the key has a NUL character
     */
    static String requireValidKey(String key) {
        return RawdataDefaultMessage.requireNoNul(java.util.Objects.requireNonNull(key, "key"), "A metadata key");
    }

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
     * @throws NullPointerException     if the key is null
     * @throws IllegalArgumentException if the key has a NUL character or is too long for the store
     * @throws RawdataClosedException   if the client is closed
     */
    byte[] get(String key);

    /**
     * Writes an entry, replacing any value the key had. The store keeps a copy of the array.
     *
     * @param key   the key
     * @param value the value
     * @return this metadata client
     * @throws NullPointerException     if the key or the value is null
     * @throws IllegalArgumentException if the key has a NUL character or is too long for the store
     * @throws RawdataClosedException   if the client is closed
     */
    RawdataMetadataClient put(String key, byte[] value);

    /**
     * Removes an entry; does nothing if there is none.
     *
     * @param key the key
     * @return this metadata client
     * @throws NullPointerException     if the key is null
     * @throws IllegalArgumentException if the key has a NUL character or is too long for the store
     * @throws RawdataClosedException   if the client is closed
     */
    RawdataMetadataClient remove(String key);
}
