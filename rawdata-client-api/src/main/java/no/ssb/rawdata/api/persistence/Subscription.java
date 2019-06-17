package no.ssb.rawdata.api.persistence;

import io.reactivex.Flowable;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.StatePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Subscription implements Disposable {

    private static final Logger LOG = LoggerFactory.getLogger(Subscription.class);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean endOfStream = new AtomicBoolean(false);
    private final PersistenceQueue<CompletedPosition> persistenceQueue;
    private final StatePersistence statePersistence;
    private final String namespace;
    private final String fromPosition;
    private final AtomicReference<Throwable> failedException = new AtomicReference<>();

    public Subscription(PersistenceQueue<CompletedPosition> persistenceQueue,
                        StatePersistence statePersistence,
                        String namespace,
                        String fromPosition) {
        this.persistenceQueue = persistenceQueue;
        this.statePersistence = statePersistence;
        this.namespace = namespace;
        this.fromPosition = fromPosition;
    }

    public Disposable subscribe(Consumer<CompletedPosition> completedPositionConsumer) {
        AtomicBoolean hasHandledFirstPosition = new AtomicBoolean();
        AtomicReference<String> fromPositionRef = new AtomicReference<>(fromPosition);

        CompletableFuture<Object> future = CompletableFuture
                .supplyAsync(() -> {
                    while (!endOfStream.get()) {
                        if (fromPositionRef.get() == null) {
                            String newFromPosition = statePersistence.getFirstPosition(namespace).blockingGet();

                            if (newFromPosition == null) {
                                nap(250L);
                                continue;
                            }
                            fromPositionRef.set(newFromPosition);
                        }

                        String toPosition = statePersistence.getLastPosition(namespace).blockingGet();

                        Flowable<CompletedPosition> flowable = statePersistence.readPositions(namespace, fromPositionRef.get(), toPosition);
                        flowable.subscribe(
                                onNext -> {
                                    if (!hasHandledFirstPosition.get()) {
                                        completedPositionConsumer.accept(onNext);
                                        fromPositionRef.set(onNext.position);
                                        hasHandledFirstPosition.set(true);
                                        return;
                                    }

                                    if (!fromPositionRef.get().equals(onNext.position)) {
                                        completedPositionConsumer.accept(onNext);
                                        fromPositionRef.set(onNext.position);
                                    }
                                },
                                failedException::set,
                                () -> {
                                }
                        );

                        if (failedException.get() != null) {
                            return null;
                        }

                        nap(250L);
                    }
                    return null;
                }, executor)
                .exceptionally(throwable -> {
                    if (!failedException.compareAndSet(null, throwable)) {
                        LOG.error("Unable to store throwable in failedException, already set. Current exception: {}", captureStackTrace(throwable));
                    }
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new RuntimeException(throwable);
                });

        executor.submit(future::join);
        return this;
    }

    private void nap(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String captureStackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }


    void shutdownAndAwaitTermination() {
        LOG.info("Shutdown Subscription..");
        endOfStream.set(true);
        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    LOG.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void cancel() {
        endOfStream.set(true);
    }

    @Override
    public void dispose() {
        close();
    }

    @Override
    public void close() {
        if (!executor.isShutdown()) {
            shutdownAndAwaitTermination();
        }
    }


}
