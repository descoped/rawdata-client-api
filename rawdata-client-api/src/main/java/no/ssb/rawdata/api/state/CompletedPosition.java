package no.ssb.rawdata.api.state;

import java.util.Objects;

public class CompletedPosition {

    public final String namespace;
    public final String position;

    public CompletedPosition(String namespace, String position) {
        this.namespace = namespace;
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompletedPosition)) return false;
        CompletedPosition that = (CompletedPosition) o;
        return Objects.equals(namespace, that.namespace) &&
                Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, position);
    }

    @Override
    public String toString() {
        return "CompletedPosition{" +
                "namespace='" + namespace + '\'' +
                ", position='" + position + '\'' +
                '}';
    }
}
