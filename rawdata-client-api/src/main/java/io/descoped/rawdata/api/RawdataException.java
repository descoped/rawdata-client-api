package io.descoped.rawdata.api;

/**
 * Base of the exceptions a rawdata client, producer, consumer or metadata client throws for a failure of the store
 * or of the operation. Invalid arguments are reported with {@link IllegalArgumentException} or
 * {@link NullPointerException} instead.
 */
public class RawdataException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception with a message.
     *
     * @param message the detail message
     */
    public RawdataException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and a cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public RawdataException(String message, Throwable cause) {
        super(message, cause);
    }
}
