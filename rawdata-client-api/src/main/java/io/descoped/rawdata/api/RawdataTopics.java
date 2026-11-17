package io.descoped.rawdata.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The rule for topic names, the same in every provider: 1 to 100 characters, letters {@code A-Z a-z}, digits, and
 * {@code _ . -}, starting with a letter or digit. A topic name becomes part of a table row, a file path or an object
 * name in the providers, so nothing else is accepted: no {@code /}, no whitespace, no {@code ..} on its own.
 */
public final class RawdataTopics {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,99}");

    private RawdataTopics() {
    }

    /**
     * Whether a name is a valid topic name.
     *
     * @param topic the name, may be null
     * @return true if the name follows the rule
     */
    public static boolean isValid(String topic) {
        return topic != null && VALID.matcher(topic).matches();
    }

    /**
     * Checks a topic name; every client method that takes a topic calls this first.
     *
     * @param topic the name
     * @return the name, unchanged
     * @throws NullPointerException     if the name is null
     * @throws IllegalArgumentException if the name breaks the rule
     */
    public static String requireValid(String topic) {
        Objects.requireNonNull(topic, "topic");
        if (!VALID.matcher(topic).matches()) {
            throw new IllegalArgumentException("Invalid topic name (1-100 of A-Z a-z 0-9 _ . -, starting with a letter or digit): '"
                    + (topic.length() > 120 ? topic.substring(0, 120) + "…" : topic) + "'");
        }
        return topic;
    }
}
