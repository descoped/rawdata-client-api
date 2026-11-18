package io.descoped.rawdata.provider.postgres.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.util.Map;
import java.util.OptionalInt;

/**
 * The provider contract on PostgreSQL 18.
 */
class PostgresContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return Postgres18.configuration();
    }

    /**
     * Below what a PostgreSQL btree index entry holds.
     */
    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of(2000);
    }

    @Override
    protected Map<String, String> missingStoreConfiguration() {
        Map<String, String> configuration = Postgres18.configuration();
        configuration.put("postgres.driver.database", "no_such_database");
        return configuration;
    }
}
