package io.descoped.rawdata.avro;

import io.descoped.rawdata.api.RawdataCursor;
import java.util.UUID;

class AvroRawdataCursor implements RawdataCursor {

    final UUID ulid;
    final boolean inclusive;

    AvroRawdataCursor(UUID ulid, boolean inclusive) {
        this.ulid = ulid;
        this.inclusive = inclusive;
    }
}
