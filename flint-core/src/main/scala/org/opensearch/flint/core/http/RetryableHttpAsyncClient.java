/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.opensearch.flint.core.FlintOptions;

/**
 * HTTP client that retries request to tolerant transient fault.
 */
public class RetryableHttpAsyncClient extends CloseableHttpAsyncClient {

  private static final Logger LOG = LoggerFactory.getLogger(RetryableHttpAsyncClient.class);

  /**
   * Delegated internal HTTP client that execute the request underlying.
   */
  private final CloseableHttpAsyncClient internalClient;

  /**
   * Flint retry options.
   */
  private final FlintRetryOptions options;

  public RetryableHttpAsyncClient(CloseableHttpAsyncClient internalClient,
                                  FlintRetryOptions options) {
    this.internalClient = internalClient;
    this.options = options;
  }

  @Override
  public boolean isRunning() {
    return internalClient.isRunning();
  }

  @Override
  public void start() {
    internalClient.start();
  }

  @Override
  public void close() throws IOException {
    internalClient.close();
  }

  @Override
  public <T> Future<T> execute(HttpAsyncRequestProducer requestProducer,
                               HttpAsyncResponseConsumer<T> responseConsumer,
                               HttpContext context,
                               FutureCallback<T> callback) {
    return new Future<>() {
      /**
       * Delegated future object created per doExecuteAndFutureGetWithRetry() call which creates initial object too.
       * In this way, we avoid the duplicate logic of first call and subsequent retry calls.
       * Here the assumption is cancel, isCancelled and isDone never called before get().
       * (OpenSearch RestClient seems only call get() API)
       */
      private Future<T> delegate;

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return delegate.isCancelled();
      }

      @Override
      public boolean isDone() {
        return delegate.isDone();
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        return doExecuteAndFutureGetWithRetry(() -> delegate.get());
      }

      @Override
      public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
        return doExecuteAndFutureGetWithRetry(() -> delegate.get(timeout, unit));
      }

      private T doExecuteAndFutureGetWithRetry(Callable<T> futureGet) throws InterruptedException, ExecutionException {
        try {
          // Retry by creating a new Future object (as new delegate) and get its result again
          return Failsafe
              .with(options.getRetryPolicy())
              .get(() -> {
                this.delegate = internalClient.execute(requestProducer, responseConsumer, context, callback);
                return futureGet.call();
              });
        } catch (FailsafeException ex) {
          LOG.error("Request failed permanently. Re-throwing original exception.");

          // Failsafe will wrap checked exception, such as ExecutionException
          // So here we have to unwrap failsafe exception and rethrow it
          Throwable cause = ex.getCause();
          if (cause instanceof InterruptedException) {
            throw (InterruptedException) cause;
          } else if (cause instanceof ExecutionException) {
            throw (ExecutionException) cause;
          } else {
            throw ex;
          }
        }
      }
    };
  }

  public static HttpAsyncClientBuilder builder(HttpAsyncClientBuilder delegate, FlintOptions options) {
    FlintRetryOptions retryOptions = options.getRetryOptions();
    if (!retryOptions.isRetryEnabled()) {
      return delegate;
    }

    // Wrap original builder so created client will be wrapped by retryable client too
    return new HttpAsyncClientBuilder() {
      @Override
      public CloseableHttpAsyncClient build() {
        LOG.info("Building retryable http async client with options: " + retryOptions);
        return new RetryableHttpAsyncClient(delegate.build(), retryOptions);
      }
    };
  }
}
