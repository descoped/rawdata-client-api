package no.ssb.rawdata.api.persistence;

import java.io.Closeable;

public interface Disposable extends Closeable {

    void cancel();

    void dispose();

    @Override
    void close();
}
