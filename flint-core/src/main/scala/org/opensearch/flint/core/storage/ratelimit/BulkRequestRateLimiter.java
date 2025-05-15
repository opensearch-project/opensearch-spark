/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

public interface BulkRequestRateLimiter {
  /**
   * Acquires the specified number of permits from the rate limiter, blocking until granted.
   *
   * @param permits number of permits (request size in bytes) to acquire
   */
  void acquirePermit(int permits);

  /**
   * Adapts rate limit based on multi signal feedback.
   */
  void adaptToFeedback(RequestFeedback feedback);

  /**
   * Returns the current rate limit.
   * This is primarily used for monitoring and testing purposes.
   */
  long getRate();

  /**
   * Sets rate limit to the given value.
   *
   * @param permitsPerSecond new rate limit in bytes per second
   */
  void setRate(long permitsPerSecond);
}
