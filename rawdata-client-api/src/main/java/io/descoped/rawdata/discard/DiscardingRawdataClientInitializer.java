package io.descoped.rawdata.discard;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.service.provider.api.ProviderName;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ProviderName("discard")
public class DiscardingRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "discard";
    }

    @Override
    public Set<String> configurationKeys() {
        return Collections.emptySet();
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new DiscardingRawdataClient();
    }
}
