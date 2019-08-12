package no.ssb.rawdata.api;

import java.util.Set;

public interface RawdataMessage {

    String position();

    Set<String> keys();

    byte[] get(String key);

    interface Builder {
        Builder position(String id);

        Builder put(String key, byte[] payload);

        RawdataMessage build();
    }
}
