package no.ssb.rawdata.api.state;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LinkedSet {

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> Set<E> of(E... elements) {
        return new LinkedHashSet<>(List.of(elements));
    }
}
