package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.service.provider.api.ProviderException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RawdataClientCreateTest {

    @Test
    void builtInProvidersAreFound() {
        for (String provider : new String[]{"memory", "discard"}) {
            try (RawdataClient client = RawdataClient.create(Map.of(RawdataClient.PROVIDER_KEY, provider))) {
                assertFalse(client.isClosed());
            }
        }
    }

    @Test
    void missingProviderIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(Map.of()));
        Map<String, String> blank = Map.of(RawdataClient.PROVIDER_KEY, " ");
        assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(blank));
        assertThrows(NullPointerException.class, () -> RawdataClient.create(null));
    }

    @Test
    void unknownProviderIsReported() {
        Map<String, String> unknown = Map.of(RawdataClient.PROVIDER_KEY, "no-such-provider");
        assertThrows(ProviderException.class, () -> RawdataClient.create(unknown));
    }
}
