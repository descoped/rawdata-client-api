package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ProviderName("memory")
public class MemoryRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "memory";
    }

    @Override
    public Set<String> configurationKeys() {
        return Collections.emptySet();
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new MemoryRawdataClient();
    }
}
