package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Reads a segment in ranges, as 2.x read a window through a seekable channel: a stream of its messages that holds one
 * range in memory, its header's summary, and its last block alone. A range that comes back wrong fails instead of
 * giving wrong messages: in the stream, Avro checks each block's sync marker; the last block must fill the rest of the
 * segment exactly and end with the last id the header names.
 */
final class AvroSegmentReader {

    /** The range a consumer reads at a time. */
    static final int STREAM_RANGE = 4 * 1024 * 1024;
    /** The first range when only the header is wanted: a header takes about 1 KiB. */
    static final int HEADER_RANGE = 8 * 1024;

    private final AvroStore.Segment segment;
    private DataFileStream<GenericRecord> stream;
    private GenericRecord reused;

    AvroSegmentReader(AvroStore.Segment segment) {
        this.segment = segment;
    }

    /**
     * The segment's summary, from its header: one small range.
     */
    AvroSegments.Summary summary() throws IOException {
        return summary(HEADER_RANGE);
    }

    /**
     * The summary, reading a first range of the given size, and larger ones if the header does not fit in it.
     */
    AvroSegments.Summary summary(int firstRange) throws IOException {
        for (int range = firstRange; ; range *= 8) {
            byte[] start = segment.read(0, (int) Math.min(range, segment.size()));
            try (DataFileStream<GenericRecord> header = AvroSegments.reader(new ByteArrayInputStream(start))) {
                return AvroSegments.summary(header);
            } catch (EOFException e) {
                if (start.length == segment.size()) {
                    throw e; // the whole segment, and still no header
                }
                // a header larger than the range: read a larger one
            }
        }
    }

    /**
     * The segment's last message: reads the header and the last block only.
     */
    RawdataMessage lastMessage() throws IOException {
        AvroSegments.Summary summary = summary();
        long lastBlock = summary.lastBlock();
        List<RawdataMessage> messages = AvroSegments.decodeBlock(segment.read(lastBlock, Math.toIntExact(segment.size() - lastBlock)));
        RawdataMessage last = messages.get(messages.size() - 1);
        if (RawdataIds.ORDER.compare(last.id(), summary.lastId()) > 0) {
            throw new IOException("The segment's last block ends with " + last.id() + ", above the greatest id its header says: "
                    + summary.lastId());
        }
        return last;
    }

    /**
     * The next message of the stream, reading the next range when the one in memory is used up.
     *
     * @return the message, or null at the end of the segment
     */
    RawdataMessage next() throws IOException {
        if (stream == null) {
            stream = AvroSegments.reader(new RangedInput(segment, STREAM_RANGE));
        }
        if (!stream.hasNext()) {
            return null;
        }
        reused = stream.next(reused);
        return AvroSegments.toMessage(reused);
    }

    /**
     * The segment as a stream, read one range at a time.
     */
    private static final class RangedInput extends InputStream {

        private final AvroStore.Segment segment;
        private final int rangeBytes;
        private byte[] range = new byte[0];
        private int inRange;
        private long next; // the offset of the next range

        RangedInput(AvroStore.Segment segment, int rangeBytes) {
            this.segment = segment;
            this.rangeBytes = rangeBytes;
        }

        private boolean fill() throws IOException {
            if (inRange < range.length) {
                return true;
            }
            if (next >= segment.size()) {
                return false;
            }
            range = segment.read(next, (int) Math.min(rangeBytes, segment.size() - next));
            if (range.length == 0) {
                throw new IOException("The store returned no bytes at " + next + " of a segment of " + segment.size());
            }
            next += range.length;
            inRange = 0;
            return true;
        }

        @Override
        public int read() throws IOException {
            return fill() ? range[inRange++] & 0xff : -1;
        }

        @Override
        public int read(byte[] into, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (!fill()) {
                return -1;
            }
            int read = Math.min(length, range.length - inRange);
            System.arraycopy(range, inRange, into, offset, read);
            inRange += read;
            return read;
        }

        @Override
        public int available() {
            return range.length - inRange;
        }
    }
}
