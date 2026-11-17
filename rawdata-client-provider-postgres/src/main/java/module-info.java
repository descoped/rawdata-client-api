/**
 * The {@code postgres} provider: topics in PostgreSQL 18, in three shared tables keyed by topic. Exports nothing; the
 * provider is found through the API module's {@code uses}.
 */
module io.descoped.rawdata.provider.postgres {
    requires java.sql;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;
    requires io.descoped.rawdata.api;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.provider.postgres.PostgresRawdataClientInitializer;
}
