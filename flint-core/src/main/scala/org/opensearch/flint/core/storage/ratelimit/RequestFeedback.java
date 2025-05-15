/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Base interface for different types of bulk request feedback events.
 * Implements the Visitor pattern to allow different handling strategies for each feedback type.
 * Feedback types include timeout, retryable failure, and success scenarios.
 */
public interface RequestFeedback {
  /**
   * Accepts a feedback handler to process this request feedback and determine a new rate limit.
   */
  long suggestNewRateLimit(FeedbackHandler handler);
}
