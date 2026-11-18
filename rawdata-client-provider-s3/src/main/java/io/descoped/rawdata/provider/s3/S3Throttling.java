package io.descoped.rawdata.provider.s3;

import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tells the Avro engine when S3 throttles, so it lowers how many uploads run at once. The SDK retries a throttled
 * request by itself, so this interceptor sees each attempt's response: 503 (S3's {@code SlowDown}) and 429. A call or
 * attempt that timed out counts too.
 */
final class S3Throttling implements ExecutionInterceptor {

    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVICE_UNAVAILABLE = 503;

    private final AtomicReference<Runnable> listener = new AtomicReference<>(() -> { });

    void onThrottled(Runnable listener) {
        this.listener.set(listener);
    }

    @Override
    public void afterTransmission(Context.AfterTransmission context, ExecutionAttributes attributes) {
        int status = context.httpResponse().statusCode();
        if (status == SERVICE_UNAVAILABLE || status == TOO_MANY_REQUESTS) {
            listener.get().run();
        }
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes attributes) {
        Throwable failure = context.exception();
        if (failure instanceof ApiCallAttemptTimeoutException || failure instanceof ApiCallTimeoutException
                || failure.getCause() instanceof SocketTimeoutException) {
            listener.get().run();
        }
    }
}
