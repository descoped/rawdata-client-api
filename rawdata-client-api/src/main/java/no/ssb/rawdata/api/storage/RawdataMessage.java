package no.ssb.rawdata.api.storage;

public interface RawdataMessage {

    RawdataMessageId id();

    RawdataMessageContent read();

}
