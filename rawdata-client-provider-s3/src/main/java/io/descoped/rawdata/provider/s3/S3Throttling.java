package io.descoped.rawdata.provider.s3;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tells the Avro engine when S3 throttles a segment upload, so it lowers how many uploads run at once. The SDK retries
 * a throttled request by itself, so this interceptor sees each attempt's response: 503 (S3's {@code SlowDown}) and
 * 429. A call or attempt that timed out counts too. Only segment puts count (the store sends them, and only them, with
 * {@code If-None-Match: *}): a read or a metadata write says nothing about the bucket's upload capacity.
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
        if (isSegmentUpload(context.request()) && (status == SERVICE_UNAVAILABLE || status == TOO_MANY_REQUESTS)) {
            listener.get().run();
        }
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes attributes) {
        Throwable failure = context.exception();
        if (isSegmentUpload(context.request()) && (failure instanceof ApiCallAttemptTimeoutException
                || failure instanceof ApiCallTimeoutException || failure.getCause() instanceof SocketTimeoutException)) {
            listener.get().run();
        }
    }

    private static boolean isSegmentUpload(SdkRequest request) {
        return request instanceof PutObjectRequest put && "*".equals(put.ifNoneMatch());
    }
}
