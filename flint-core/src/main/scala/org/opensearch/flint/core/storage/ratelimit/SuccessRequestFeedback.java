/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Represents a successful bulk request completion.
 * Includes latency information to help detect performance issues even in successful requests.
 */
public class SuccessRequestFeedback implements RequestFeedback {

  /** Request latency in milliseconds for the request */
  public final long latencyMillis;

  public SuccessRequestFeedback(long latencyMillis) {
    this.latencyMillis = latencyMillis;
  }

  /**
   * Creates a new instance.
   *
   * @param latencyMillis the request latency in milliseconds
   */
  public static SuccessRequestFeedback create(long latencyMillis) {
    return new SuccessRequestFeedback(latencyMillis);
  }

  @Override
  public long suggestNewRateLimit(FeedbackHandler handler) {
    return handler.handleFeedback(this);
  }
}
