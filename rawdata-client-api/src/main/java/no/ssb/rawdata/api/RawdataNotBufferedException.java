package no.ssb.rawdata.api;

public class RawdataNotBufferedException extends RuntimeException {
    public RawdataNotBufferedException() {
    }

    public RawdataNotBufferedException(String message) {
        super(message);
    }

    public RawdataNotBufferedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawdataNotBufferedException(Throwable cause) {
        super(cause);
    }

    public RawdataNotBufferedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
