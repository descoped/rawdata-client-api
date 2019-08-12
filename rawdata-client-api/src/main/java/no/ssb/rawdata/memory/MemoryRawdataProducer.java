package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class MemoryRawdataProducer implements RawdataProducer {

    private final MemoryRawdataTopic topic;

    private final Map<String, MemoryRawdataMessageContent> buffer = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Consumer<MemoryRawdataProducer> closeAction;

    MemoryRawdataProducer(MemoryRawdataTopic topic, Consumer<MemoryRawdataProducer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public String lastPosition() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            MemoryRawdataMessageId lastMessageId = topic.lastMessageId();
            if (lastMessageId == null) {
                return null;
            }
            return topic.read(lastMessageId).content().position();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new RawdataMessage.Builder() {
            String position;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessage.Builder position(String position) {
                this.position = position;
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public MemoryRawdataMessageContent build() {
                return new MemoryRawdataMessageContent(position, data);
            }
        };
    }

    @Override
    public MemoryRawdataMessageContent buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public MemoryRawdataMessageContent buffer(RawdataMessage message) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(message.position(), (MemoryRawdataMessageContent) message);
        return (MemoryRawdataMessageContent) message;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataContentNotBufferedException {
        for (String position : positions) {
            MemoryRawdataMessageContent content = buffer.remove(position);
            if (content == null) {
                throw new RawdataContentNotBufferedException(String.format("position %s has not been buffered", position));
            }
            topic.tryLock(5, TimeUnit.SECONDS);
            try {
                topic.write(content);
            } finally {
                topic.unlock();
            }
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(positions));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeAction.accept(this);
        closed.set(true);
    }
}
