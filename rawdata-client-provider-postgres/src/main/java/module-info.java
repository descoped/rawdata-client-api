import io.descoped.rawdata.provider.postgres.PostgresRawdataClientInitializer;

module io.descoped.rawdata.postgres {
    requires java.logging;
    requires org.slf4j;
    requires java.sql;
    requires de.huxhorn.sulky.ulid;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;
    requires io.descoped.config;
    requires io.descoped.rawdata.api;
    requires io.descoped.service.provider.api;

    opens io.descoped.rawdata.provider.postgres.init;

    provides RawdataClientInitializer with PostgresRawdataClientInitializer;
}
