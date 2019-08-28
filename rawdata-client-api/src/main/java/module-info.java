import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;

module no.ssb.rawdata.api {
    requires transitive no.ssb.service.provider.api;
    requires de.huxhorn.sulky.ulid;

    exports no.ssb.rawdata.api;

    provides RawdataClientInitializer with MemoryRawdataClientInitializer;
}
