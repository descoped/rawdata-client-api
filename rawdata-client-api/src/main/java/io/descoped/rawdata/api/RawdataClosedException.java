package io.descoped.rawdata.api;

/**
 * Thrown when the client, producer or consumer it was called on is closed.
 */
public class RawdataClosedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception without a message.
     */
    public RawdataClosedException() {
    }

    /**
     * Creates the exception with a message.
     *
     * @param message the detail message
     */
    public RawdataClosedException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and a cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public RawdataClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates the exception with a cause.
     *
     * @param cause the cause
     */
    public RawdataClosedException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates the exception with full control over suppression and the stack trace.
     *
     * @param message            the detail message
     * @param cause              the cause
     * @param enableSuppression  whether suppression is enabled
     * @param writableStackTrace whether the stack trace is writable
     */
    public RawdataClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
