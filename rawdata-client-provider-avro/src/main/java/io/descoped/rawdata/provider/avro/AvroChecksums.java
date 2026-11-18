package io.descoped.rawdata.provider.avro;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.zip.CRC32C;

/**
 * The checksum stores send with an upload, so the service refuses bytes damaged on the way.
 */
public final class AvroChecksums {

    private AvroChecksums() {
    }

    /**
     * The CRC32C of bytes as GCS ({@code X-Goog-Hash: crc32c=}) and S3 ({@code x-amz-checksum-crc32c}) write it:
     * base64 of the 4 bytes, big-endian.
     *
     * @param content the bytes
     * @return the checksum
     */
    public static String crc32c(byte[] content) {
        CRC32C crc = new CRC32C();
        crc.update(content);
        return Base64.getEncoder().encodeToString(ByteBuffer.allocate(4).putInt((int) crc.getValue()).array());
    }
}
