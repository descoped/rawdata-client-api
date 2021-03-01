package no.ssb.rawdata.api;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

class RawdataSubscriber implements Flow.Subscriber<RawdataMessage.Builder> {

    final Supplier<RawdataProducer> producerSupplier;
    final AtomicReference<RawdataProducer> producerRef = new AtomicReference<>();
    final AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
    final AtomicLong received = new AtomicLong();
    final BiConsumer<Throwable, RawdataProducer> onError;
    final Consumer<RawdataProducer> onComplete;

    RawdataSubscriber(Supplier<RawdataProducer> producerSupplier, Consumer<RawdataProducer> onComplete, BiConsumer<Throwable, RawdataProducer> onError) {
        this.producerSupplier = producerSupplier;
        this.onError = onError;
        this.onComplete = onComplete;
    }

    RawdataSubscriber(Supplier<RawdataProducer> producerSupplier, Consumer<RawdataProducer> onComplete) {
        this.producerSupplier = producerSupplier;
        this.onError = null;
        this.onComplete = onComplete;
    }

    RawdataSubscriber(Supplier<RawdataProducer> producerSupplier, BiConsumer<Throwable, RawdataProducer> onError) {
        this.producerSupplier = producerSupplier;
        this.onError = onError;
        this.onComplete = null;
    }

    RawdataSubscriber(Supplier<RawdataProducer> producerSupplier) {
        this.producerSupplier = producerSupplier;
        this.onError = null;
        this.onComplete = null;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (!subscriptionRef.compareAndSet(null, subscription)) {
            subscription.cancel();
            return;
        }
        producerRef.set(producerSupplier.get());
        subscription.request(10);
    }

    @Override
    public void onNext(RawdataMessage.Builder item) {
        RawdataProducer producer = producerRef.get();
        producer.buffer(item);
        producer.publish(item.position());
        if (received.incrementAndGet() % 5L == 0L) {
            subscriptionRef.get().request(5);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            Objects.requireNonNull(throwable);
            if (onError != null) {
                onError.accept(throwable, producerRef.get());
            }
        } finally {
            close();
        }
    }

    @Override
    public void onComplete() {
        try {
            if (onComplete != null) {
                onComplete.accept(producerRef.get());
            }
        } finally {
            close();
        }
    }

    private void close() {
        subscriptionRef.set(null);
        RawdataProducer producer = producerRef.getAndSet(null);
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
