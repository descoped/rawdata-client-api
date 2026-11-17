package io.descoped.rawdata.api;

/**
 * Thrown when a client, producer, consumer or metadata client is used after it, or the client it belongs to, was
 * closed.
 */
public class RawdataClosedException extends RawdataException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception with a default message.
     */
    public RawdataClosedException() {
        super("closed");
    }

    /**
     * Creates the exception with a message.
     *
     * @param message the detail message
     */
    public RawdataClosedException(String message) {
        super(message);
    }
}
