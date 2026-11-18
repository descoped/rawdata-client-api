package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataCursor;

import java.util.UUID;

/**
 * A start position: the id to start from, and whether the message with that id is included.
 */
final class AvroCursor implements RawdataCursor {

    final UUID id;
    final boolean inclusive;

    AvroCursor(UUID id, boolean inclusive) {
        this.id = id;
        this.inclusive = inclusive;
    }

    @Override
    public String toString() {
        return "AvroCursor{id=" + id + ", inclusive=" + inclusive + '}';
    }
}
