package no.ssb.rawdata.api.storage;

import io.reactivex.Flowable;
import no.ssb.rawdata.api.state.CompletedPosition;

import java.io.Closeable;
import java.util.Set;

/**
 * Rawdata Client provides I/O for the rawdata store.
 */
public interface RawdataClient extends Closeable {

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
    Set<String> list(String namespace, String fromPosition, String toPosition);

    /**
     * Get the first position for namespace
     *
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
     * Get the next position for namespace
     *
     * @return
     */
    String nextPosition(String namespace);

    /**
     * Publish expected positions in a guaranteed sequence that will be written to the bucket
     *
     * @param namespace
     * @param expectedPositions
     */
    void publishExpectedPositions(String namespace, Set<String> expectedPositions);

    /**
     * Publish completed positions in a guaranteed sequence that has been written to the bucket
     *
     * @param namespace
     * @param completedPositions
     */
    void publishCompletedPositions(String namespace, Set<String> completedPositions);

    /**
     * Subscribe to new completed positions. The subscriber is guaranteed to receive positions in an ordered sequence.
     *
     * @param namespace
     * @param fromPosition
     * @return
     */
    Flowable<CompletedPosition> subscribe(String namespace, String fromPosition);

}
