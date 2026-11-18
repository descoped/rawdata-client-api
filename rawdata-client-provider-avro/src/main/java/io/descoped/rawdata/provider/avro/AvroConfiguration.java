package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMetadataClient;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Reads the configuration of a provider built on the engine, with the same messages for every provider.
 */
public final class AvroConfiguration {

    private AvroConfiguration() {
    }

    /**
     * Returns a required value.
     *
     * @param configuration the configuration
     * @param key           the key
     * @return the value
     * @throws IllegalArgumentException if the value is missing or blank
     */
    public static String required(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing configuration '" + key + "'");
        }
        return value.trim();
    }

    /**
     * Returns an optional value.
     *
     * @param configuration the configuration
     * @param key           the key
     * @param defaultValue  the value when the key is missing or blank
     * @return the value
     */
    public static String optional(Map<String, String> configuration, String key, String defaultValue) {
        String value = configuration.get(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    /**
     * Returns an optional positive number.
     *
     * @param configuration the configuration
     * @param key           the key
     * @param defaultValue  the value when the key is missing or blank
     * @return the number
     * @throws IllegalArgumentException if the value is not a positive number
     */
    public static long positive(Map<String, String> configuration, String key, long defaultValue) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            long number = Long.parseLong(value.trim());
            if (number > 0) {
                return number;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        throw new IllegalArgumentException("Configuration '" + key + "' must be a positive number: " + value);
    }

    /**
     * The longest object-name prefix in UTF-8 bytes: an object name of 1024 bytes then still holds the longest topic
     * (100), {@code /metadata/k} and a metadata key of {@link RawdataMetadataClient#PORTABLE_KEY_BYTES} in hex.
     */
    public static final int MAX_PREFIX_BYTES = 1024 - (100 + "/metadata/k".length()) - 2 * RawdataMetadataClient.PORTABLE_KEY_BYTES;

    /**
     * An optional folder prefix for object names, for stores whose names are at most 1024 bytes (GCS, S3): empty, or
     * ending with {@code /} (added if missing).
     *
     * @param configuration the configuration
     * @param key           the key of the prefix
     * @return the prefix, empty or ending with {@code /}
     * @throws IllegalArgumentException if it starts with {@code /} or is longer than {@link #MAX_PREFIX_BYTES}
     */
    public static String prefix(Map<String, String> configuration, String key) {
        String prefix = optional(configuration, key, "");
        if (prefix.isEmpty()) {
            return prefix;
        }
        if (prefix.startsWith("/")) {
            throw new IllegalArgumentException("Configuration '" + key + "' must not start with '/': " + prefix);
        }
        String folder = prefix.endsWith("/") ? prefix : prefix + "/";
        if (folder.getBytes(StandardCharsets.UTF_8).length > MAX_PREFIX_BYTES) {
            throw new IllegalArgumentException("Configuration '" + key + "' is longer than " + MAX_PREFIX_BYTES
                    + " bytes, so metadata keys of " + RawdataMetadataClient.PORTABLE_KEY_BYTES + " bytes would not fit");
        }
        return folder;
    }
}
