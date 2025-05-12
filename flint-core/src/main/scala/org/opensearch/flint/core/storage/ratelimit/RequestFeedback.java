/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Encapsulates feedback information about a bulk request's outcome for rate limiting purposes.
 * This class provides information about request timeouts, retryable failures, and latency that
 * the rate limiter uses to adapt its rate.
 *
 * The class provides factory methods for common request outcomes:
 * - noRetryable: for successful requests or non-retryable failures
 * - hasRetryable: for requests with retryable failures
 * - timeout: for request that timed out
 */
public class RequestFeedback {
  /** Indicates if the request timed out */
  public final boolean isTimeout;
  /** Indicates if the request failed with a retryable error */
  public final boolean hasRetryableFailure;
  /** The request latency in milliseconds */
  public final long latency;

  /**
   * Creates a new RequestFeedback instance.
   *
   * @param isTimeout whether the request timed out
   * @param hasRetryableFailure whether the request had a retryable failure
   * @param latency the request latency in milliseconds
   */
  public RequestFeedback(boolean isTimeout, boolean hasRetryableFailure, long latency) {
    this.isTimeout = isTimeout;
    this.hasRetryableFailure = hasRetryableFailure;
    this.latency = latency;
  }

  /**
   * Creates feedback for a successful request or non-retryable failures.
   *
   * @param latency the request latency in milliseconds
   */
  public static RequestFeedback noRetryable(long latency) {
    return new RequestFeedback(false, false, latency);
  }

  /**
   * Creates feedback for a request with retryable failures.
   *
   * @param latency the request latency in milliseconds
   */
  public static RequestFeedback hasRetryable(long latency) {
    return new RequestFeedback(false, true, latency);
  }

  /**
   * Creates feedback for a request that timed out.
   */
  public static RequestFeedback timeout() {
    return new RequestFeedback(true, false, 0);
  }
}
