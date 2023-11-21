/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.newSetFromMap;
import static java.util.logging.Level.SEVERE;

import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import dev.failsafe.function.CheckedPredicate;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;

/**
 * Flint options related to HTTP request retry.
 */
public class FlintRetryOptions {

  private static final Logger LOG = Logger.getLogger(FlintRetryOptions.class.getName());

  /**
   * All Flint options.
   */
  private final Map<String, String> options;

  /**
   * Maximum retry attempt
   */
  public static final int DEFAULT_MAX_RETRIES = 3;
  public static final String MAX_RETRIES = "retry.max_retries";

  /**
   * Retryable exception class name
   */
  public static final String RETRYABLE_EXCEPTION_CLASS_NAMES = "retry.exception_class_names";

  public FlintRetryOptions(Map<String, String> options) {
    this.options = options;
  }

  /**
   * Build retry policy based on the given Flint options.
   *
   * @param <T> success execution result type
   * @return Failsafe retry policy
   */
  public <T> RetryPolicy<T> getRetryPolicy() {
    LOG.info("Building HTTP request retry policy with retry options: " + this);
    RetryPolicyBuilder<T> builder =
        RetryPolicy.<T>builder()
            .withMaxRetries(getMaxRetries())
            .withBackoff(1, 30, SECONDS)
            .withJitter(Duration.ofMillis(100))
            // .handleResultIf(resp -> ((HttpResponse<T>) resp).statusCode() == 200)
            .onFailedAttempt(ex ->
                LOG.log(SEVERE, "Attempt to execute request failed", ex.getLastException()))
            .onRetry(ex ->
                LOG.warning("Retrying failed request at #" + ex.getAttemptCount()));

    // Add optional retryable exception handler
    if (options.containsKey(RETRYABLE_EXCEPTION_CLASS_NAMES)) {
      builder.handleIf(isRetryableException());
    } else {
      // By default, Failsafe handles any Exception
      builder.handleIf(ex -> false);
    }
    return builder.build();
  }

  private int getMaxRetries() {
    return Integer.parseInt(
        options.getOrDefault(MAX_RETRIES, String.valueOf(DEFAULT_MAX_RETRIES)));
  }

  private CheckedPredicate<? extends Throwable> isRetryableException() {
    // Use weak collection avoids blocking class unloading
    Set<Class<? extends Throwable>> retryableExceptions = newSetFromMap(new WeakHashMap<>());
    Arrays.stream(options.get(RETRYABLE_EXCEPTION_CLASS_NAMES).split(","))
        .map(String::trim)
        .map(this::loadClass)
        .forEach(retryableExceptions::add);

    // Consider retryable if found anywhere on error stacktrace
    return throwable -> {
      // Handle nested exception to avoid dead loop
      Set<Throwable> seen = new HashSet<>();
      while (throwable != null && seen.add(throwable)) {
        for (Class<? extends Throwable> retryable : retryableExceptions) {
          if (retryable.isInstance(throwable)) {
            return true;
          }
        }
        throwable = throwable.getCause();
      }
      return false;
    };
  }

  private Class<? extends Throwable> loadClass(String className) {
    try {
      //noinspection unchecked
      return (Class<? extends Throwable>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load class " + className, e);
    }
  }

  @Override
  public String toString() {
    return "FlintRetryOptions{" +
        "maxRetries=" + getMaxRetries() +
        ", retryableExceptionClassNames=" +
        options.getOrDefault(RETRYABLE_EXCEPTION_CLASS_NAMES, "") +
        '}';
  }
}
