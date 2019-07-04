import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.memory.MemoryRawdataClient;

module no.ssb.rawdata.api {
    exports no.ssb.rawdata.api;

    provides RawdataClient with MemoryRawdataClient;
}
