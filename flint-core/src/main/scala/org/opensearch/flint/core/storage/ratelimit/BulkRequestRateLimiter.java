/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

public interface BulkRequestRateLimiter {
  void acquirePermit();

  void acquirePermit(int permits);

  long getRate();

  void adaptToFeedback(RequestFeedback feedback);

  void setRate(long permitsPerSecond);
}
