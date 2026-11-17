package io.descoped.rawdata.api;

/**
 * Thrown by {@link RawdataClient#cursorOf(String, String, boolean, long, java.time.Duration)} when no message with the
 * position exists in the searched time range of the topic.
 */
public class RawdataNoSuchPositionException extends RawdataException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception with a message.
     *
     * @param message the detail message
     */
    public RawdataNoSuchPositionException(String message) {
        super(message);
    }
}
