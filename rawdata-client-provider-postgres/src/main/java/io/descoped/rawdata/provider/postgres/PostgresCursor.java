package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataCursor;

import java.util.Objects;
import java.util.UUID;

public class PostgresCursor implements RawdataCursor {

    /**
     * Need not exactly match an existing ulid-value.
     */
    final UUID startKey;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    final boolean inclusive;

    PostgresCursor(UUID startKey, boolean inclusive) {
        this.startKey = startKey;
        this.inclusive = inclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostgresCursor that = (PostgresCursor) o;
        return inclusive == that.inclusive &&
                Objects.equals(startKey, that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, inclusive);
    }

    @Override
    public String toString() {
        return "PostgresCursor{" +
                "startKey=" + startKey +
                ", inclusive=" + inclusive +
                '}';
    }
}
