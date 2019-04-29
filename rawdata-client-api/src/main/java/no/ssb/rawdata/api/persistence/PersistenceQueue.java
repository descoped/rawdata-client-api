package no.ssb.rawdata.api.persistence;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import no.ssb.config.DynamicConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PersistenceQueue<T> implements Closeable {

    private final DynamicConfiguration configuration;
    private final BlockingQueue<T> blockingQueue;
    private final T noMoreUpStreamItemsAllowed;
    private volatile boolean closed;

    public PersistenceQueue(DynamicConfiguration configuration) {
        this.configuration = configuration;
        blockingQueue = new LinkedBlockingQueue<>();
        noMoreUpStreamItemsAllowed = newNoMoreUpStreamItemsAllowed();
    }

    public <T> T newNoMoreUpStreamItemsAllowed() {
        try {
            return (T) Class.forName(getClass().getGenericSuperclass().getTypeName()).getDeclaredConstructor(new Class[]{}).newInstance(new Object[]{});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void enqueue(T data) {
        if (closed) {
            throw new IllegalStateException("The queue provider is closed!");
        }
        blockingQueue.add(data);
    }

    public Flowable<T> toFlowable() {
        return Flowable.create(emitter -> {
            try {
                while (true) {
                    if (emitter.isCancelled()) {
                        return;
                    }

                    if (closed) {
                        emitter.onComplete();
                        return;
                    }

                    if (emitter.requested() == 0) {
                        return;
                    }

                    T task = blockingQueue.take();
                    if (task == noMoreUpStreamItemsAllowed) {
                        blockingQueue.add(noMoreUpStreamItemsAllowed); // ensure that multiple subscribers get a complete signal
                        emitter.onComplete();
                        return;
                    }
                    emitter.onNext(task);
                }
            } catch (Throwable t) {
                emitter.onError(t);
            }
        }, BackpressureStrategy.BUFFER);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        blockingQueue.add(noMoreUpStreamItemsAllowed);
    }

}
