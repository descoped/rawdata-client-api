package no.ssb.rawdata.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class MemoryRawdataTopic {

    final String topic;
    final List<MemoryRawdataMessage> data = new ArrayList<>(); // protected by lock
    final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();

    MemoryRawdataTopic(String topic) {
        this.topic = topic;
    }

    MemoryRawdataMessageId lastMessageId() {
        checkHasLock();
        if (data.isEmpty()) {
            return null;
        }
        return data.get(data.size() - 1).id();
    }

    private void checkHasLock() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("The calling thread must hold the lock before calling this method");
        }
    }

    MemoryRawdataMessageId write(MemoryRawdataMessageContent content) {
        checkHasLock();
        MemoryRawdataMessageId id = new MemoryRawdataMessageId(topic, data.size());
        data.add(new MemoryRawdataMessage(id, copy(content))); // use copy to fake serialization and deserialization
        signalProduction();
        return id;
    }

    private MemoryRawdataMessageContent copy(MemoryRawdataMessageContent original) {
        return new MemoryRawdataMessageContent(original.externalId(),
                original.keys().stream().collect(Collectors.toMap(
                        k -> k, k -> {
                            byte[] src = original.get(k);
                            byte[] dest = new byte[src.length];
                            System.arraycopy(src, 0, dest, 0, src.length);
                            return src;
                        }
                ))
        );
    }

    boolean hasNext(MemoryRawdataMessageId id) {
        checkHasLock();
        if (id.index + 1 >= data.size()) {
            return false;
        }
        return true;
    }

    MemoryRawdataMessage readNext(MemoryRawdataMessageId id) {
        checkHasLock();
        return data.get(id.index + 1);
    }

    public MemoryRawdataMessage read(MemoryRawdataMessageId id) {
        checkHasLock();
        return data.get(id.index);
    }

    boolean isLegalPosition(MemoryRawdataMessageId id) {
        int nextIndex = id.index + 1;
        if (nextIndex < 0) {
            return false;
        }
        if (nextIndex > data.size()) {
            return false;
        }
        return true;
    }

    void tryLock() {
        try {
            if (!lock.tryLock(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("timeout while waiting for lock");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void unlock() {
        lock.unlock();
    }

    void awaitProduction(long duration, TimeUnit unit) throws InterruptedException {
        condition.await(duration, unit);
    }

    void signalProduction() {
        condition.signalAll();
    }

    @Override
    public String toString() {
        return "MemoryRawdataTopic{" +
                "topic='" + topic + '\'' +
                '}';
    }
}
