import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.discard.DiscardingRawdataClientInitializer;
import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;

module no.ssb.rawdata.api {
    requires transitive no.ssb.service.provider.api;
    requires transitive de.huxhorn.sulky.ulid;
    requires org.slf4j;

    exports no.ssb.rawdata.api;

    provides RawdataClientInitializer with MemoryRawdataClientInitializer, DiscardingRawdataClientInitializer;
}
