import io.descoped.rawdata.discard.DiscardingRawdataClientInitializer;
import io.descoped.rawdata.memory.MemoryRawdataClientInitializer;

module io.descoped.rawdata.api {
    requires transitive io.descoped.service.provider.api;
    requires transitive de.huxhorn.sulky.ulid;
    requires org.slf4j;

    exports io.descoped.rawdata.api;

    provides RawdataClientInitializer with MemoryRawdataClientInitializer, DiscardingRawdataClientInitializer;
}
