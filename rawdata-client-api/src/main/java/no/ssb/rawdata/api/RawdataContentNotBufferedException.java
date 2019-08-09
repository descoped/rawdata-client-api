package no.ssb.rawdata.api;

public class RawdataContentNotBufferedException extends RuntimeException {
    public RawdataContentNotBufferedException() {
    }

    public RawdataContentNotBufferedException(String message) {
        super(message);
    }

    public RawdataContentNotBufferedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawdataContentNotBufferedException(Throwable cause) {
        super(cause);
    }

    public RawdataContentNotBufferedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
