/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Represents a timeout event in bulk request processing.
 */
public class TimeoutRequestFeedback implements RequestFeedback {

  /**
   * Creates a new instance.
   */
  public static TimeoutRequestFeedback create() {
    return new TimeoutRequestFeedback();
  }

  @Override
  public long suggestNewRateLimit(FeedbackHandler handler) {
    return handler.handleFeedback(this);
  }
}
