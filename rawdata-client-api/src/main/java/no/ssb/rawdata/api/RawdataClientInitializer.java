package no.ssb.rawdata.api;

import java.util.Map;

public interface RawdataClientInitializer {

    RawdataClient initialize(Map<String, String> configuration);
}
