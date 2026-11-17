package io.descoped.rawdata.avro;


import io.descoped.rawdata.api.RawdataIds;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AvroFileMetadata {
    final AtomicReference<UUID> idOfFirstRecord = new AtomicReference<>();
    final AtomicReference<String> positionOfFirstRecord = new AtomicReference<>();
    final AtomicLong count = new AtomicLong(0);
    final AtomicLong syncOfLastBlock = new AtomicLong(0);

    void clear() {
        idOfFirstRecord.set(null);
        positionOfFirstRecord.set(null);
        count.set(0);
        syncOfLastBlock.set(0);
    }

    void incrementCounter(long increment) {
        count.addAndGet(increment);
    }

    long getCount() {
        return count.get();
    }

    void setSyncOfLastBlock(long position) {
        syncOfLastBlock.set(position);
    }

    long getSyncOfLastBlock() {
        return syncOfLastBlock.get();
    }

    void setPositionOfFirstRecord(String position) {
        positionOfFirstRecord.compareAndSet(null, position);
    }

    String getPositionOfFirstRecord() {
        return positionOfFirstRecord.get();
    }

    void setIdOfFirstRecord(UUID id) {
        idOfFirstRecord.compareAndSet(null, id);
    }

    UUID getIdOfFirstRecord() {
        return idOfFirstRecord.get();
    }

    public String toFilename() {
        String fromTime = AvroRawdataUtils.formatTimestamp(RawdataIds.timestamp(getIdOfFirstRecord()));
        return fromTime + "_" + getCount() + "_" + getSyncOfLastBlock() + "_" + getPositionOfFirstRecord() + ".avro";
    }

    public abstract RawdataAvroFile toRawdataAvroFile(String topic);
}
