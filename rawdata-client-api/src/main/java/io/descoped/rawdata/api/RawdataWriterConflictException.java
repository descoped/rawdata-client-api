package io.descoped.rawdata.api;

/**
 * Thrown by a publish that finds that another client published to the topic since this client last wrote or looked:
 * another process writes the topic. Only one process writes a topic (several producers of its client may); the
 * publish writes nothing, and this client keeps failing to publish to the topic, so the stream is never merged from
 * two writers. A new client, for example after the other writer stopped, starts from the topic's current end.
 */
public class RawdataWriterConflictException extends RawdataException {

    private static final long serialVersionUID = 1L;

    /**
     * The topic another process published to.
     */
    private final String topic;

    /**
     * Creates the exception for a topic.
     *
     * @param topic the topic another process published to
     */
    public RawdataWriterConflictException(String topic) {
        super("Another client published to topic '" + topic + "': only one process may write a topic");
        this.topic = topic;
    }

    /**
     * Returns the topic another process published to.
     *
     * @return the topic
     */
    public String topic() {
        return topic;
    }
}
