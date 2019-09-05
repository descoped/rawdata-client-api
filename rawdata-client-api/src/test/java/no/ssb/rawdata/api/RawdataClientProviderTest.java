package no.ssb.rawdata.api;

import no.ssb.rawdata.discard.DiscardingRawdataClient;
import no.ssb.rawdata.memory.MemoryRawdataClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RawdataClientProviderTest {

    @Test
    public void thatMemoryAndNoneRawdataClientsAreAvailableThroughServiceProviderMechanism() {
        {
            RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
            assertNotNull(client);
            assertTrue(client instanceof MemoryRawdataClient);
        }
        {
            RawdataClient client = ProviderConfigurator.configure(Map.of(), "discard", RawdataClientInitializer.class);
            assertNotNull(client);
            assertTrue(client instanceof DiscardingRawdataClient);
        }
    }
}
