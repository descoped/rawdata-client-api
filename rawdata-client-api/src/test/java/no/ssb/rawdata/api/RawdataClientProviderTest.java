package no.ssb.rawdata.api;

import no.ssb.rawdata.memory.MemoryRawdataClient;
import org.testng.annotations.Test;

import java.util.ServiceLoader;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class RawdataClientProviderTest {

    @Test
    public void thatRawdataClientIsAvailableThroughServiceProviderMechanism() {
        ServiceLoader<RawdataClient> loader = ServiceLoader.load(RawdataClient.class);
        RawdataClient client = loader.stream().findFirst().orElseThrow().get();
        assertNotNull(client);
        assertTrue(client instanceof MemoryRawdataClient);
    }
}
