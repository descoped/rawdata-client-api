package no.ssb.rawdata.api.storage;

import io.reactivex.Flowable;
import no.ssb.rawdata.api.persistence.Disposable;

import java.io.Closeable;
import java.util.List;
import java.util.function.Consumer;

/**
 * Rawdata Client provides I/O for the rawdata store.
 */
public interface RawdataClient<T> extends Closeable {

    /**
     * Read file from bucket
     *
     * @param namespace
     * @param position
     * @param filename
     * @return
     */
    byte[] read(String namespace, String position, String filename);

    /**
     * Write file to bucket
     *
     * @param namespace
     * @param position
     * @param filename
     * @param data
     */
    void write(String namespace, String position, String filename, byte[] data);

    /**
     * Resolve a range of valid position
     *
     * @param namespace
     * @param fromPosition
     * @param toPosition
     * @return
     */
    List<String> list(String namespace, String fromPosition, String toPosition);

    /**
     * Get the first position for namespace
     *
     * @param namespace
     * @return
     */
    String firstPosition(String namespace);

    /**
     * Get the last position for namespace
     *
     * @param namespace
     * @return
     */
    String lastPosition(String namespace);

    /**
     * Find offset position
     *
     * @param namespace
     * @param fromPosition The a given from position
     * @param offset Number of positions ahead
     * @return Offset position
     */
    String offsetPosition(String namespace, String fromPosition, int offset);

    /**
     * Publish completed positions in a guaranteed sequence that has been written to the bucket
     *
     * @param namespace
     * @param completedPositions
     */
    void publish(String namespace, List<String> completedPositions);

    /**
     * Subscribe to new completed positions. The subscriber is guaranteed to receive positions in an ordered sequence.
     *
     * @param namespace
     * @param fromPosition
     * @return
     */
    @Deprecated
    Flowable<T> subscription(String namespace, String fromPosition);

    /**
     * Subscribe to new completed positions. The subscriber is guaranteed to receive positions in an ordered sequence.
     *
     * @param namespace
     * @param fromPosition
     * @param completedPosition
     */
    Disposable subscribe(String namespace, String fromPosition, Consumer<T> completedPosition);

}
