/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.http;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.logging.Level.SEVERE;

import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
  public static final int DEFAULT_MAX_ATTEMPT = 4; // TODO: change to max retry
  public static final String MAX_ATTEMPT = "retry.max_attempt";

  /**
   * Retryable exception class name
   */
  public static final String RETRYABLE_EXCEPTION_CLASS_NAMES = "retry.exception_class_names";

  public FlintRetryOptions(Map<String, String> options) {
    this.options = options;
  }

  public <T> RetryPolicy<T> getRetryPolicy() {
    RetryPolicy<T> policy =
        RetryPolicy.<T>builder()
            .withMaxAttempts(getMaxAttempt())
            .withBackoff(1, 30, SECONDS)
            .withJitter(Duration.ofMillis(100))
            .handleIf(isRetryableException())
            .onFailedAttempt(
                ex -> LOG.log(SEVERE, "Attempt failed", ex.getLastException()))
            .onRetry(
                ex -> LOG.warning("Failure #{}. Retrying at " + ex.getAttemptCount()))
            .build();

    LOG.info("Built HTTP request retry policy with max attempt: "
        + policy.getConfig().getMaxRetries());
    return policy;
  }

  private int getMaxAttempt() {
    return Integer.parseInt(
        options.getOrDefault(MAX_ATTEMPT, String.valueOf(DEFAULT_MAX_ATTEMPT)));
  }

  private CheckedPredicate<? extends Throwable> isRetryableException() {
    // Populate configured exception class name from options
    Set<String> retryableClassNames;
    if (options.containsKey(RETRYABLE_EXCEPTION_CLASS_NAMES)) {
      retryableClassNames =
          Arrays.stream(options.get(RETRYABLE_EXCEPTION_CLASS_NAMES).split(","))
              .map(String::trim)
              .collect(Collectors.toSet());
    } else {
      retryableClassNames = new HashSet<>();
    }

    // Add default retryable exception class names
    retryableClassNames.add(ConnectException.class.getName());

    // Consider retryable if found anywhere on error stacktrace
    return throwable -> {
      // Handle nested exception to avoid dead loop
      Set<Throwable> seen = new HashSet<>();
      while (throwable != null && seen.add(throwable)) {
        if (retryableClassNames.contains(throwable.getClass().getName())) {
          return true;
        }
        throwable = throwable.getCause();
      }
      return false;
    };
  }
}
