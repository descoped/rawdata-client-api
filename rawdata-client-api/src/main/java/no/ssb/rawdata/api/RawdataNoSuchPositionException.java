package no.ssb.rawdata.api;

public class RawdataNoSuchPositionException extends RuntimeException {
    public RawdataNoSuchPositionException() {
    }

    public RawdataNoSuchPositionException(String message) {
        super(message);
    }

    public RawdataNoSuchPositionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RawdataNoSuchPositionException(Throwable cause) {
        super(cause);
    }

    public RawdataNoSuchPositionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
