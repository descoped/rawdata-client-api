package io.descoped.rawdata.discard;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.service.provider.api.ProviderName;

import java.util.Map;

/**
 * The {@code discard} provider: accepts everything and keeps nothing. No configuration.
 */
@ProviderName("discard")
public class DiscardingRawdataClientInitializer implements RawdataClientInitializer {

    /**
     * Creates the initializer; called by the service loader.
     */
    public DiscardingRawdataClientInitializer() {
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new DiscardingRawdataClient();
    }
}
