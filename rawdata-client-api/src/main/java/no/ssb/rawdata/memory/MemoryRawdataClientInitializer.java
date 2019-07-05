package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClientInitializer;

import java.util.Map;

public class MemoryRawdataClientInitializer implements RawdataClientInitializer {

    public MemoryRawdataClient initialize(Map<String, String> configuration) {
        return new MemoryRawdataClient();
    }
}
