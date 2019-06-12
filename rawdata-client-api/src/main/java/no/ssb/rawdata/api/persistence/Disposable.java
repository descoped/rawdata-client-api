package no.ssb.rawdata.api.persistence;

public interface Disposable {

    void cancel();

    void dispose();

}
