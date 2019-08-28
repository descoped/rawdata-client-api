package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

class MemoryRawdataTopic {

    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> previousUlid = new AtomicReference<>(ulid.nextValue(System.currentTimeMillis()));
    final String topic;
    final NavigableMap<ULID.Value, MemoryRawdataMessage> data = new ConcurrentSkipListMap<>(); // protected by lock
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
        return data.lastEntry().getValue().id();
    }

    private void checkHasLock() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("The calling thread must hold the lock before calling this method");
        }
    }

    MemoryRawdataMessageId write(MemoryRawdataMessageContent content) {
        checkHasLock();
        ULID.Value prev;
        ULID.Value key;
        do {
            prev = this.previousUlid.get();
            key = ulid.nextStrictlyMonotonicValue(prev, System.currentTimeMillis()).orElseThrow();
        } while (!previousUlid.compareAndSet(prev, key));
        MemoryRawdataMessageId id = new MemoryRawdataMessageId(topic, key);
        data.put(key, new MemoryRawdataMessage(id, copy(content))); // use copy to fake serialization and deserialization
        signalProduction();
        return id;
    }

    private MemoryRawdataMessageContent copy(MemoryRawdataMessageContent original) {
        return new MemoryRawdataMessageContent(original.position(),
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
        return data.higherKey(id.ulid) != null;
    }

    MemoryRawdataMessage readNext(MemoryRawdataMessageId id) {
        checkHasLock();
        return ofNullable(data.higherEntry(id.ulid)).map(Map.Entry::getValue).orElse(null);
    }

    public MemoryRawdataMessage read(MemoryRawdataMessageId id) {
        checkHasLock();
        return data.get(id.ulid);
    }

    boolean isLegalPosition(MemoryRawdataMessageId id) {
        return data.containsKey(id.ulid);
    }

    boolean tryLock() {
        return lock.tryLock();
    }

    void tryLock(int timeout, TimeUnit unit) {
        try {
            if (!lock.tryLock(timeout, unit)) {
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

    public MemoryRawdataMessageId findPositionOfTimestamp(long timestamp) {
        ULID.Value ulidValue = ulid.nextValue(timestamp);
        ULID.Value lowerBound = new ULID.Value(ulidValue.getMostSignificantBits() & 0xFFFFFFFFFFFF0000L, 0L); // first value with timestamp
        return new MemoryRawdataMessageId(topic, lowerBound);
    }
}
