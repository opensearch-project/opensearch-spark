/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

/**
 * Defines the contract for handling different types of RequestFeedback events. Implements the
 * Visitor pattern to process each type of feedback differently. Handlers determine new rate limits
 * based on the feedback type and its properties.
 */
public interface FeedbackHandler {

  /**
   * Handles timeout feedback and suggests a new rate limit.
   */
  long handleFeedback(TimeoutRequestFeedback feedback);

  /**
   * Handles retryable failure feedback and suggests a new rate limit.
   */
  long handleFeedback(RetryableFailureRequestFeedback feedback);

  /**
   * Handles successful request feedback and suggests a new rate limit.
   */
  long handleFeedback(SuccessRequestFeedback feedback);
}
