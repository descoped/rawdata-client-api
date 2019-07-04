package no.ssb.rawdata.api;

import java.util.Set;

public interface RawdataMessageContent {

    String externalId();

    Set<String> keys();

    byte[] get(String key);
}
