package no.ssb.rawdata.api;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class RawdataSubscriber implements Flow.Subscriber<RawdataMessage.Builder> {

    final Supplier<RawdataProducer> producerSupplier;
    final AtomicReference<RawdataProducer> producerRef = new AtomicReference<>();
    final AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
    final AtomicLong received = new AtomicLong();

    RawdataSubscriber(Supplier<RawdataProducer> producerSupplier) {
        this.producerSupplier = producerSupplier;
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
        Objects.requireNonNull(throwable);
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

    @Override
    public void onComplete() {
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
