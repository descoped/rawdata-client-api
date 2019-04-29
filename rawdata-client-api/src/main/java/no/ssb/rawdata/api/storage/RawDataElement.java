package no.ssb.rawdata.api.storage;

public class RawDataElement {
    final String namespace;
    final String position;
    final String filename;
    final int offset;
    final int byteBuffer;

    public RawDataElement(String namespace, String position, String filename, int offset, int byteBuffer) {
        this.namespace = namespace;
        this.position = position;
        this.filename = filename;
        this.offset = offset;
        this.byteBuffer = byteBuffer;
    }
}
