package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemoryRawdataProducer implements RawdataProducer {

    private final MemoryRawdataTopic topic;

    private final Map<String, MemoryRawdataMessageContent> buffer = new ConcurrentHashMap<>();

    MemoryRawdataProducer(MemoryRawdataTopic topic) {
        this.topic = topic;
    }

    @Override
    public String lastExternalId() {
        topic.tryLock();
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
    public RawdataMessageContent.Builder builder() {
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
    public MemoryRawdataMessageContent buffer(RawdataMessageContent.Builder builder) {
        return buffer(builder.build());
    }

    @Override
    public MemoryRawdataMessageContent buffer(RawdataMessageContent content) {
        buffer.put(content.externalId(), (MemoryRawdataMessageContent) content);
        return (MemoryRawdataMessageContent) content;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> externalIds) {
        return publish(externalIds.toArray(new String[externalIds.size()]));
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... externalIds) {
        topic.tryLock();
        try {
            List<MemoryRawdataMessageId> messageIds = new ArrayList<>();
            for (String position : externalIds) {
                MemoryRawdataMessageContent content = buffer.remove(position);
                MemoryRawdataMessageId messageId = topic.write(content);
                messageIds.add(messageId);
            }
            return messageIds;
        } finally {
            topic.unlock();
        }
    }

    @Override
    public void close() {
    }
}
