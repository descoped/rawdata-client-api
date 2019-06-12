package no.ssb.rawdata.api.state;

import no.ssb.rawdata.api.persistence.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SubscriptionThreadTest {

    @Test
    public void testName1() throws InterruptedException {
        Disposable disposable = subscribe(completedPosition -> {
            System.out.printf("Hello: %s%n", completedPosition);
        });

        Thread.sleep(2000L);

        System.out.printf("Test1%n");

        disposable.dispose();
    }

    @Test
    public void testName2() throws InterruptedException {
        System.out.printf("Test2%n");
        Thread.sleep(2000L);
    }

    private Disposable subscribe(Consumer<CompletedPosition> completedPositionConsumer) {
        return new Subscription().subscribe(completedPositionConsumer);
    }

    static class Subscription implements Disposable, Closeable {
        private static final Logger LOG = LoggerFactory.getLogger(Subscription.class);
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AtomicBoolean endOfStream = new AtomicBoolean(false);

        public Subscription() {
        }

        public Disposable subscribe(Consumer<CompletedPosition> completedPositionConsumer) {
            CompletableFuture<CompletedPosition> future = CompletableFuture
                    .supplyAsync(() -> {
                        CompletedPosition completedPosition = new CompletedPosition(null, null);
                        while (!endOfStream.get()) {
                            completedPositionConsumer.accept(completedPosition);
                            try {
                                Thread.sleep(250L);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return completedPosition;
                    }, executor)
                    .whenComplete((completedPosition, throwable) -> {
                        LOG.warn("{}", completedPosition);
                    });

            executor.submit(future::join);
            return this;
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
}
