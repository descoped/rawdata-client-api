package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataCursor;

import java.util.Objects;
import java.util.UUID;

/**
 * A starting point: an id, which need not be the id of a message, and whether a message with exactly that id is read
 * first.
 */
final class MemoryCursor implements RawdataCursor {

    final UUID startKey;
    final boolean inclusive;

    MemoryCursor(UUID startKey, boolean inclusive) {
        this.startKey = Objects.requireNonNull(startKey, "id");
        this.inclusive = inclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MemoryCursor)) {
            return false;
        }
        MemoryCursor that = (MemoryCursor) o;
        return inclusive == that.inclusive && startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, inclusive);
    }

    @Override
    public String toString() {
        return "MemoryCursor{startKey=" + startKey + ", inclusive=" + inclusive + '}';
    }
}
