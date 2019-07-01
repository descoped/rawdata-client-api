package no.ssb.rawdata.api.storage;

public interface RawdataMessageContent {

    String position();

    byte[] read(String filename);

}
