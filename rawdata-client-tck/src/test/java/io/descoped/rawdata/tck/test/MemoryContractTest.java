package io.descoped.rawdata.tck.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.util.Map;

class MemoryContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return Map.of(RawdataClient.PROVIDER_KEY, "memory");
    }

    /**
     * A memory client keeps its own topics, so there is no second client on the same store.
     */
    @Override
    protected boolean clientsShareTheStore() {
        return false;
    }
}
