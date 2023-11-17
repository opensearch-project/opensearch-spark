/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;

/**
 * Flint HTTP client with fault tolerance.
 */
public class FlintHttpClient extends CloseableHttpAsyncClient {

  private static final Logger LOG = Logger.getLogger(FlintHttpClient.class.getName());

  private final CloseableHttpAsyncClient client;

  public FlintHttpClient(CloseableHttpAsyncClient client) {
    this.client = client;
  }

  @Override
  public boolean isRunning() {
    return client.isRunning();
  }

  @Override
  public void start() {
    client.start();
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public <T> Future<T> execute(HttpAsyncRequestProducer requestProducer, HttpAsyncResponseConsumer<T> responseConsumer, HttpContext context, FutureCallback<T> callback) {
    return new RetryFuture<>(() -> client.execute(requestProducer, responseConsumer, context, callback));
  }

  public static class Builder extends HttpAsyncClientBuilder {

    private final HttpAsyncClientBuilder builder;

    public Builder(HttpAsyncClientBuilder builder) {
      this.builder = builder;
    }

    @Override
    public CloseableHttpAsyncClient build() {
      return new FlintHttpClient(builder.build());
    }
  }

  public static class RetryFuture<T> implements Future<T> {

    /**
     * Action that executes the request and get a new Future
     */
    private final Supplier<Future<T>> retry;

    /**
     * Initial Future object
     */
    private final Future<T> future;

    public RetryFuture(Supplier<Future<T>> retry) {
      this.retry = retry;
      this.future = retry.get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      try {
        return future.get();
      } catch (Exception e) {
        LOG.warning("Checking if exception can be retried");
        return Failsafe
            .with(buildRetryPolicy())
            .get(() -> retry.get().get());
      }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      try {
        return future.get(timeout, unit);
      } catch (Exception e) {
        LOG.warning("Checking if exception can be retried");
        return Failsafe
            .with(buildRetryPolicy())
            .get(() -> retry.get().get(timeout, unit));
      }
    }

    private RetryPolicy<T> buildRetryPolicy() {
      return RetryPolicy.<T>builder()
          .withMaxRetries(3)
          .withBackoff(1, 30, SECONDS)
          .withJitter(Duration.ofMillis(100))
          .handleIf(ex -> ex.getCause() instanceof ConnectException)
          .onFailedAttempt(
              ex -> LOG.log(SEVERE, "Attempt failed", ex.getLastException()))
          .onRetry(
              ex -> LOG.warning("Failure #{}. Retrying at " + ex.getAttemptCount()))
          .build();
    }
  }
}



