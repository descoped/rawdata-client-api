package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    public String lastExternalId() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            MemoryRawdataMessageId lastMessageId = topic.lastMessageId();
            if (lastMessageId == null) {
                return null;
            }
            return topic.read(lastMessageId).content().externalId();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public RawdataMessageContent.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new RawdataMessageContent.Builder() {
            String externalId;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessageContent.Builder externalId(String externalId) {
                this.externalId = externalId;
                return this;
            }

            @Override
            public RawdataMessageContent.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public MemoryRawdataMessageContent build() {
                return new MemoryRawdataMessageContent(externalId, data);
            }
        };
    }

    @Override
    public MemoryRawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public MemoryRawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(content.externalId(), (MemoryRawdataMessageContent) content);
        return (MemoryRawdataMessageContent) content;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        return publish(externalIds.toArray(new String[externalIds.size()]));
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        try {
            List<CompletableFuture<? extends RawdataMessageId>> futures = publishAsync(externalIds);
            List<MemoryRawdataMessageId> result = new ArrayList<>();
            for (CompletableFuture<? extends RawdataMessageId> future : futures) {
                result.add((MemoryRawdataMessageId) future.join());
            }
            return result;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RawdataContentNotBufferedException) {
                throw (RawdataContentNotBufferedException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    @Override
    public List<CompletableFuture<? extends RawdataMessageId>> publishAsync(String... externalIds) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            List<CompletableFuture<? extends RawdataMessageId>> futures = new ArrayList<>();
            for (String externalId : externalIds) {
                MemoryRawdataMessageContent content = buffer.remove(externalId);
                if (content == null) {
                    throw new RawdataContentNotBufferedException(String.format("externalId %s has not been buffered", externalId));
                }
                MemoryRawdataMessageId messageId = topic.write(content);
                futures.add(CompletableFuture.completedFuture(messageId));
            }
            return futures;
        } finally {
            topic.unlock();
        }
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
