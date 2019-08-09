package no.ssb.rawdata.api;

public class RawdataClosedException extends RuntimeException {
    public RawdataClosedException() {
    }

    public RawdataClosedException(String message) {
        super(message);
    }

    public RawdataClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawdataClosedException(Throwable cause) {
        super(cause);
    }

    public RawdataClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
