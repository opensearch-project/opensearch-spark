/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Represents a timeout event in bulk request processing.
 * Uses singleton pattern since timeout feedback doesn't need additional state.
 */
public class TimeoutRequestFeedback implements RequestFeedback {

  private static final TimeoutRequestFeedback instance = new TimeoutRequestFeedback();

  /**
   * Returns the singleton instance.
   */
  public static TimeoutRequestFeedback create() {
    return instance;
  }

  @Override
  public long suggestNewRateLimit(FeedbackHandler handler) {
    return handler.handleFeedback(this);
  }
}
