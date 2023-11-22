/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.newSetFromMap;
import static java.util.logging.Level.SEVERE;

import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;

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

  public static final String DEFAULT_RETRYABLE_HTTP_STATUS_CODES = "429,502";
  public static final String RETRYABLE_HTTP_STATUS_CODES = "retry.http_status_codes";

  /**
   * Retryable exception class name
   */
  public static final String RETRYABLE_EXCEPTION_CLASS_NAMES = "retry.exception_class_names";

  public FlintRetryOptions(Map<String, String> options) {
    this.options = options;
  }

  /**
   * Is auto retry capability enabled.
   *
   * @return true if enabled, otherwise false.
   */
  public boolean isRetryEnabled() {
    return getMaxRetries() > 0;
  }

  /**
   * Build retry policy based on the given Flint options.
   *
   * @param <T> success execution result type
   * @return Failsafe retry policy
   */
  public <T> RetryPolicy<T> getRetryPolicy() {
    return RetryPolicy.<T>builder()
        // Backoff strategy config (can be configurable as needed in future)
        .withBackoff(1, 30, SECONDS)
        .withJitter(Duration.ofMillis(100))
        // Failure handling config from Flint options
        .withMaxRetries(getMaxRetries())
        .handleIf(getRetryableExceptionHandler())
        .handleResultIf(getRetryableResultHandler())
        // Logging listener
        .onFailedAttempt(ex ->
            LOG.log(SEVERE, "Attempt to execute request failed", ex.getLastException()))
        .onRetry(ex ->
            LOG.warning("Retrying failed request at #" + ex.getAttemptCount())).build();
  }

  /**
   * @return maximum retry option value
   */
  public int getMaxRetries() {
    return Integer.parseInt(
        options.getOrDefault(MAX_RETRIES, String.valueOf(DEFAULT_MAX_RETRIES)));
  }

  /**
   * @return retryable HTTP status code list
   */
  public String getRetryableHttpStatusCodes() {
    return options.getOrDefault(RETRYABLE_HTTP_STATUS_CODES, DEFAULT_RETRYABLE_HTTP_STATUS_CODES);
  }

  /**
   * @return retryable exception class name list
   */
  public Optional<String> getRetryableExceptionClassNames() {
    return Optional.ofNullable(options.get(RETRYABLE_EXCEPTION_CLASS_NAMES));
  }

  private <T> CheckedPredicate<T> getRetryableResultHandler() {
    Set<Integer> retryableStatusCodes =
        Arrays.stream(getRetryableHttpStatusCodes().split(","))
            .map(String::trim)
            .map(Integer::valueOf)
            .collect(Collectors.toSet());

    return result -> retryableStatusCodes.contains(
        ((HttpResponse) result).getStatusLine().getStatusCode());
  }

  private CheckedPredicate<? extends Throwable> getRetryableExceptionHandler() {
    // By default, Failsafe handles any Exception
    Optional<String> exceptionClassNames = getRetryableExceptionClassNames();
    if (exceptionClassNames.isEmpty()) {
      return ex -> false;
    }

    // Use weak collection avoids blocking class unloading
    Set<Class<? extends Throwable>> retryableExceptions = newSetFromMap(new WeakHashMap<>());
    Arrays.stream(exceptionClassNames.get().split(","))
        .map(String::trim)
        .map(this::loadClass)
        .forEach(retryableExceptions::add);

    // Consider retryable if exception found anywhere on stacktrace.
    // Meanwhile, handle nested exception to avoid dead loop by seen hash set.
    return throwable -> {
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
        ", retryableStatusCodes=" + getRetryableHttpStatusCodes() +
        ", retryableExceptionClassNames=" + getRetryableExceptionClassNames() +
        '}';
  }
}
