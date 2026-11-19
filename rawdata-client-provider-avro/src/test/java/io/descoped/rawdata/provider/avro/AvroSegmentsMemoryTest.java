package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opening a segment's Avro file costs about one block, not Avro's buffer for the largest sync interval (1 GiB), and
 * Avro still never ends a block by itself.
 */
class AvroSegmentsMemoryTest {

    private static final int BLOCK_BYTES = 1024 * 1024;

    /**
     * The bytes this thread has allocated, from the JDK's {@code com.sun.management.ThreadMXBean}. Reached by
     * reflection: the tests are compiled into the provider's module, which does not read {@code jdk.management}.
     */
    private static long allocatedBytes() throws ReflectiveOperationException {
        Object threads = Class.forName("java.lang.management.ManagementFactory").getMethod("getThreadMXBean").invoke(null);
        Method allocated = Class.forName("com.sun.management.ThreadMXBean").getMethod("getCurrentThreadAllocatedBytes");
        return (long) allocated.invoke(threads);
    }

    @Test
    void openingASegmentAllocatesAboutOneBlock() throws IOException, ReflectiveOperationException {
        long before = allocatedBytes();
        try (DataFileWriter<GenericRecord> writer = AvroSegments.writer(1, new UUID(0, 0))) {
            AvroSegments.create(writer, new ByteArrayOutputStream(), BLOCK_BYTES);
            long allocated = allocatedBytes() - before;
            assertTrue(allocated < 8L * BLOCK_BYTES, "allocated " + allocated + " bytes");
        }
    }

    @Test
    void avroDoesNotEndABlockByItself() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = AvroSegments.writer(1, new UUID(0, 0))) {
            AvroSegments.create(writer, out, 64); // a tiny expected block
            UUID id = RawdataIds.beginningOfTime();
            for (int i = 0; i < 100; i++) {
                writer.append(AvroSegments.toRecord(RawdataMessage.builder().id(id = RawdataIds.next(id))
                        .position("p" + i).put("payload", new byte[100]).build()));
            }
        }
        try (DataFileStream<GenericRecord> in = new DataFileStream<>(new ByteArrayInputStream(out.toByteArray()),
                new GenericDatumReader<>())) {
            in.hasNext();
            assertEquals(100, in.getBlockCount(), "one block of 100 messages");
        }
    }
}
