import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.api.storage.RawdataClientInitializer;

module no.ssb.rawdata.api {
    requires no.ssb.config;
    requires no.ssb.service.provider.api;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;
    requires java.sql;
    requires org.slf4j;

    uses StatePersistenceInitializer;
    uses RawdataClientInitializer;

    exports no.ssb.rawdata.api.persistence;
    exports no.ssb.rawdata.api.state;
    exports no.ssb.rawdata.api.storage;
    exports no.ssb.rawdata.api.util;
}
