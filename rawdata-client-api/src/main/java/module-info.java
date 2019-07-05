import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;

module no.ssb.rawdata.api {
    exports no.ssb.rawdata.api;

    provides RawdataClientInitializer with MemoryRawdataClientInitializer;
}
