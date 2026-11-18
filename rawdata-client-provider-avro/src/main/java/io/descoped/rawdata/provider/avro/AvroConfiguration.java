package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMetadataClient;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Reads the configuration of a provider built on the engine, with the same messages for every provider. Values are
 * read as dynamic-configuration's getters read them: numbers strictly, enums case-insensitive with {@code -} as
 * {@code _}; a default covers a missing key, never a malformed value; every error names the key.
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
     * Returns an optional positive number. As in dynamic-configuration, the default covers a missing key only: a value
     * that is not a number, blank included, is an error that names the key.
     *
     * @param configuration the configuration
     * @param key           the key
     * @param defaultValue  the value when the key is missing
     * @return the number
     * @throws IllegalArgumentException if the value is not a positive number
     */
    public static long positive(Map<String, String> configuration, String key, long defaultValue) {
        long number = number(configuration, key, defaultValue);
        if (number < 1) {
            throw new IllegalArgumentException("Configuration '" + key + "' must be a positive number: " + number);
        }
        return number;
    }

    /**
     * Returns an optional number that is 0 or more, read as {@link #positive} reads.
     *
     * @param configuration the configuration
     * @param key           the key
     * @param defaultValue  the value when the key is missing
     * @return the number
     * @throws IllegalArgumentException if the value is not a number of 0 or more
     */
    public static long nonNegative(Map<String, String> configuration, String key, long defaultValue) {
        long number = number(configuration, key, defaultValue);
        if (number < 0) {
            throw new IllegalArgumentException("Configuration '" + key + "' must be 0 or more: " + number);
        }
        return number;
    }

    private static long number(Map<String, String> configuration, String key, long defaultValue) {
        String value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value); // strict: no blanks, no spaces, no units
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Configuration '" + key + "' must be a whole number: '" + value + "'");
        }
    }

    /**
     * Returns an optional enum value, read as dynamic-configuration reads enums: case-insensitive, with {@code -}
     * read as {@code _}, so {@code compute-engine} and {@code COMPUTE_ENGINE} are the same.
     *
     * @param configuration the configuration
     * @param key           the key
     * @param type          the enum
     * @param defaultValue  the value when the key is missing
     * @param <E>           the enum type
     * @return the value
     * @throws IllegalArgumentException if the value is none of the enum's
     */
    public static <E extends Enum<E>> E enumValue(Map<String, String> configuration, String key, Class<E> type, E defaultValue) {
        String value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        String name = value.trim().replace('-', '_');
        for (E constant : type.getEnumConstants()) {
            if (constant.name().equalsIgnoreCase(name)) {
                return constant;
            }
        }
        StringBuilder allowed = new StringBuilder();
        for (E constant : type.getEnumConstants()) {
            allowed.append(allowed.length() == 0 ? "" : ", ").append(constant.name().toLowerCase(Locale.ROOT).replace('_', '-'));
        }
        throw new IllegalArgumentException("Configuration '" + key + "' must be one of " + allowed + ": '" + value + "'");
    }

    /**
     * Returns the poll interval of a waiting consumer from a 2.x listing key, in whole seconds: 0 polls as often as
     * a receive asks, but no more often than every {@value #MIN_POLL_MILLIS} ms.
     *
     * @param configuration  the configuration
     * @param key            the key, for example {@code listing.min-interval-seconds}
     * @param defaultSeconds the value when the key is missing
     * @return the interval in milliseconds, at least {@value #MIN_POLL_MILLIS}
     * @throws IllegalArgumentException if the value is not a number of 0 or more
     */
    public static long pollIntervalMillis(Map<String, String> configuration, String key, long defaultSeconds) {
        long seconds = nonNegative(configuration, key, defaultSeconds);
        return Math.max(MIN_POLL_MILLIS, Math.multiplyExact(seconds, 1000L));
    }

    /**
     * The shortest poll interval, in milliseconds.
     */
    public static final long MIN_POLL_MILLIS = 10;

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
