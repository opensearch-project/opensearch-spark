/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Represents a retryable failure in bulk request processing.
 * Includes latency information to help with rate limiting decisions.
 */
public class RetryableFailureRequestFeedback implements RequestFeedback {

  /** Request latency in milliseconds for the request */
  public final long latencyMillis;

  public RetryableFailureRequestFeedback(long latencyMillis) {
    this.latencyMillis = latencyMillis;
  }

  /**
   * Creates a new instance.
   *
   * @param latencyMillis the request latency in milliseconds
   */
  public static RetryableFailureRequestFeedback create(long latencyMillis) {
    return new RetryableFailureRequestFeedback(latencyMillis);
  }

  @Override
  public long suggestNewRateLimit(FeedbackHandler handler) {
    return handler.handleFeedback(this);
  }
}
