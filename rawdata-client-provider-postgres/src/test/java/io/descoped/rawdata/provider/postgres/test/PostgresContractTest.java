package io.descoped.rawdata.provider.postgres.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.util.Map;

/**
 * The provider contract on PostgreSQL 18.
 */
class PostgresContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return Postgres18.configuration();
    }
}
