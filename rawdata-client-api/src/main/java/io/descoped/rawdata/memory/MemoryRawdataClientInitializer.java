package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.service.provider.api.ProviderName;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ProviderName("memory")
public class MemoryRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public Set<String> configurationKeys() {
        return Collections.emptySet();
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new MemoryRawdataClient();
    }
}
