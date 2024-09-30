package io.descoped.rawdata.api;

import io.descoped.rawdata.discard.DiscardingRawdataClient;
import io.descoped.rawdata.memory.MemoryRawdataClient;
import io.descoped.service.provider.api.ProviderConfigurator;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RawdataClientProviderTest {

    @Test
    public void thatMemoryAndNoneRawdataClientsAreAvailableThroughServiceProviderMechanism() {
        {
            io.descoped.rawdata.api.RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
            assertNotNull(client);
            assertTrue(client instanceof MemoryRawdataClient);
        }
        {
            io.descoped.rawdata.api.RawdataClient client = ProviderConfigurator.configure(Map.of(), "discard", RawdataClientInitializer.class);
            assertNotNull(client);
            assertTrue(client instanceof DiscardingRawdataClient);
        }
    }
}
