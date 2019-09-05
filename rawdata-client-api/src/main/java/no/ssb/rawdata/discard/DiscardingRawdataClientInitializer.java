package no.ssb.rawdata.discard;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

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
