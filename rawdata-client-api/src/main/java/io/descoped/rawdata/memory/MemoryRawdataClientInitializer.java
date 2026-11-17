package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.service.provider.api.ProviderName;

import java.util.Map;

/**
 * The {@code memory} provider: topics in memory, gone when the client is closed. No configuration.
 */
@ProviderName("memory")
public class MemoryRawdataClientInitializer implements RawdataClientInitializer {

    /**
     * Creates the initializer; called by the service loader.
     */
    public MemoryRawdataClientInitializer() {
        // nothing to set up: the client is created in initialize
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new MemoryRawdataClient();
    }
}
