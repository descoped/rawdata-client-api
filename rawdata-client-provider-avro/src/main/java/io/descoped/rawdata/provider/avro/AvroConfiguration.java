package io.descoped.rawdata.provider.avro;

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
}
