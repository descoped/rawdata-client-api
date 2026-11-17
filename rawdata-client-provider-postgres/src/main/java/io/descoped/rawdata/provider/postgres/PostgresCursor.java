package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataCursor;

import java.util.Objects;
import java.util.UUID;

/**
 * A starting point: an id, which need not be the id of a message, and whether a message with exactly that id is read
 * first.
 */
final class PostgresCursor implements RawdataCursor {

    final UUID id;
    final boolean inclusive;

    PostgresCursor(UUID id, boolean inclusive) {
        this.id = Objects.requireNonNull(id, "id");
        this.inclusive = inclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PostgresCursor)) {
            return false;
        }
        PostgresCursor that = (PostgresCursor) o;
        return inclusive == that.inclusive && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, inclusive);
    }

    @Override
    public String toString() {
        return "PostgresCursor{id=" + id + ", inclusive=" + inclusive + '}';
    }
}
