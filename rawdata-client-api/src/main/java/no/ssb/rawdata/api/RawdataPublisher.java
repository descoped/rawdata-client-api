package no.ssb.rawdata.api;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class RawdataPublisher implements Flow.Publisher<RawdataMessage> {

    static final RawdataMessage DUMMY = RawdataMessage.builder()
            .ulid(new ULID.Value(0L, 0L))
            .position("dummy")
            .build();

    final Supplier<RawdataConsumer> consumerSupplier;

    RawdataPublisher(Supplier<RawdataConsumer> consumerSupplier) {
        this.consumerSupplier = consumerSupplier;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super RawdataMessage> subscriber) {
        Objects.requireNonNull(subscriber);
        Subscription subscription;
        try {
            subscription = new Subscription(subscriber);
        } catch (Throwable t) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                }

                @Override
                public void cancel() {
                }
            });
            subscriber.onError(t);
            return;
        }
        subscriber.onSubscribe(subscription);
    }

    class Subscription implements Flow.Subscription {

        final AtomicLong budget = new AtomicLong();
        final AtomicLong emitted = new AtomicLong();
        final AtomicBoolean inRequest = new AtomicBoolean();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final AtomicBoolean closed = new AtomicBoolean();

        final AtomicReference<RawdataConsumer> consumerRef = new AtomicReference<>();

        final Flow.Subscriber<? super RawdataMessage> subscriber;

        public Subscription(Flow.Subscriber<? super RawdataMessage> subscriber) {
            this.subscriber = subscriber;
            RawdataConsumer consumer = consumerSupplier.get();
            Objects.requireNonNull(consumer);
            consumerRef.set(consumer);
        }

        @Override
        public void request(long n) {
            if (!cancelled.get() && n <= 0L) {
                subscriber.onError(new IllegalArgumentException("requested number of items must be > 0"));
                cancel();
                return;
            }

            long b;
            long delta;
            do {
                b = budget.get();
                delta = Math.min(Long.MAX_VALUE - b, n);
            } while (!budget.compareAndSet(b, b + delta)); // busy-wait add in case of race-condition

            if (!inRequest.compareAndSet(false, true)) {
                return;
            }

            try {
                RawdataConsumer consumer = consumerRef.get();
                RawdataMessage item = DUMMY;
                while (!cancelled.get() && emitted.get() < budget.get() && (item = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                    subscriber.onNext(item);
                    emitted.incrementAndGet();
                }

                if (item == null) {
                    // end of stream
                    if (closed.compareAndSet(false, true)) {
                        consumer.close();
                        consumerRef.set(null);
                    }
                    subscriber.onComplete();
                    cancelled.set(true);
                }

            } catch (Throwable t) {
                cancelled.set(true);
                subscriber.onError(t);
            } finally {
                inRequest.set(false);
            }

            if (cancelled.get()) {
                if (closed.compareAndSet(false, true)) {
                    try {
                        consumerRef.get().close();
                        consumerRef.set(null);
                    } catch (Throwable t) {
                        // ignore
                        t.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void cancel() {
            cancelled.set(true);
            request(0);
        }
    }
}
