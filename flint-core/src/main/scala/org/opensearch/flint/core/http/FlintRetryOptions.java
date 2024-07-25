/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;

import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import org.opensearch.flint.core.http.handler.ExceptionClassNameFailurePredicate;
import org.opensearch.flint.core.http.handler.HttpStatusCodeResultPredicate;
import java.io.Serializable;

/**
 * Flint options related to HTTP request retry.
 */
public class FlintRetryOptions implements Serializable {

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
        .handleIf(ExceptionClassNameFailurePredicate.create(getRetryableExceptionClassNames()))
        .handleResultIf(new HttpStatusCodeResultPredicate<>(getRetryableHttpStatusCodes()))
        // Logging listener
        .onFailedAttempt(event ->
            LOG.severe("Attempt to execute request failed: " + event))
        .onRetry(ex ->
            LOG.warning("Retrying failed request at #" + ex.getAttemptCount()))
        .build();
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

  @Override
  public String toString() {
    return "FlintRetryOptions{" +
        "maxRetries=" + getMaxRetries() +
        ", retryableStatusCodes=" + getRetryableHttpStatusCodes() +
        ", retryableExceptionClassNames=" + getRetryableExceptionClassNames() +
        '}';
  }
}
