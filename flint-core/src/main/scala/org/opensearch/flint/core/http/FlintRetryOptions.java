/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.opensearch.flint.core.FlintOptions.SERVICE_NAME;
import static org.opensearch.flint.core.FlintOptions.SERVICE_NAME_AOSS;
import static org.opensearch.flint.core.FlintOptions.SERVICE_NAME_ES;

import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import dev.failsafe.event.ExecutionAttemptedEvent;
import dev.failsafe.function.CheckedPredicate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.flint.core.http.handler.ExceptionClassNameFailurePredicate;
import org.opensearch.flint.core.http.handler.HttpAOSSResultPredicate;
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
  public static final int DEFAULT_BULK_MAX_RETRIES = 10;
  public static final String BULK_MAX_RETRIES = "retry.bulk.max_retries";
  public static final int DEFAULT_BULK_INITIAL_BACKOFF = 4;
  public static final String BULK_INITIAL_BACKOFF = "retry.bulk.initial_backoff";

  public static final String DEFAULT_RETRYABLE_HTTP_STATUS_CODES = "429,500,502";
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
    RetryPolicyBuilder<T> builder = RetryPolicy.<T>builder()
        // Backoff strategy config (can be configurable as needed in future)
        .withBackoff(1, 30, SECONDS)
        .withJitter(Duration.ofMillis(100))
        // Failure handling config from Flint options
        .withMaxRetries(getMaxRetries())
        .handleIf(ExceptionClassNameFailurePredicate.create(getRetryableExceptionClassNames()))
        .handleResultIf(new HttpStatusCodeResultPredicate<>(getRetryableHttpStatusCodes()))
        // Logging listener
        .onFailedAttempt(FlintRetryOptions::onFailure)
        .onRetry(FlintRetryOptions::onRetry);
    if (SERVICE_NAME_AOSS.equals(getServiceName())) {
      builder.handleResultIf(new HttpAOSSResultPredicate<>());
    }
    return builder.build();
  }

  public RetryPolicy<BulkResponse> getBulkRetryPolicy(CheckedPredicate<BulkResponse> resultPredicate) {
    return RetryPolicy.<BulkResponse>builder()
        // Using higher initial backoff to mitigate throttling quickly
        .withBackoff(getBulkInitialBackoff(), 30, SECONDS)
        .withJitter(Duration.ofMillis(100))
        .withMaxRetries(getBulkMaxRetries())
        // Do not retry on exception (will be handled by the other retry policy TODO
        // .handleIf(ExceptionClassNameFailurePredicate.create( Optional.ofNullable("java.net.ConnectException,java.net.SocketTimeoutException")))
        .handleIf((ex) -> false)
        .handleResultIf(resultPredicate)
        .onFailedAttempt(FlintRetryOptions::onFailure)
        .onRetry(FlintRetryOptions::onRetry)
        .build();
  }

  private static <T> void onFailure(ExecutionAttemptedEvent<T> event) {
    LOG.severe("Attempt to execute request failed: " + event);
  }

  private static <T> void onRetry(ExecutionAttemptedEvent<T> event) {
    LOG.warning("Retrying failed request at #" + event.getAttemptCount());
  }

  private String getServiceName() {
    return options.getOrDefault(SERVICE_NAME, SERVICE_NAME_ES);
  }

  /**
   * @return maximum retry option value
   */
  public int getMaxRetries() {
    return Integer.parseInt(
        options.getOrDefault(MAX_RETRIES, String.valueOf(DEFAULT_MAX_RETRIES)));
  }

  /**
   * @return bulk maximum retry option value
   */
  public int getBulkMaxRetries() {
    return Integer.parseInt(
        options.getOrDefault(BULK_MAX_RETRIES, String.valueOf(DEFAULT_BULK_MAX_RETRIES)));
  }

  /**
   * @return maximum retry option value
   */
  public int getBulkInitialBackoff() {
    return Integer.parseInt(
        options.getOrDefault(BULK_INITIAL_BACKOFF, String.valueOf(DEFAULT_BULK_INITIAL_BACKOFF)));
  }

  public Set<Integer> getRetryableHttpStatusCodes() {
    String statusCodes = options.getOrDefault(RETRYABLE_HTTP_STATUS_CODES, DEFAULT_RETRYABLE_HTTP_STATUS_CODES);
    return Arrays.stream(statusCodes.split(","))
        .map(String::trim)
        .map(Integer::valueOf)
        .collect(Collectors.toSet());
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
