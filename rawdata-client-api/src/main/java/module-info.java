module io.descoped.rawdata.api {
    requires transitive io.descoped.service.provider.api;
    requires transitive de.huxhorn.sulky.ulid;
    requires org.slf4j;

    exports io.descoped.rawdata.api;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.memory.MemoryRawdataClientInitializer,
            io.descoped.rawdata.discard.DiscardingRawdataClientInitializer;
}
