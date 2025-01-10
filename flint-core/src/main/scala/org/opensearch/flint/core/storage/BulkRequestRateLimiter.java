/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

public interface BulkRequestRateLimiter {
  void acquirePermit();

  void acquirePermit(int permits);

  void increaseRate();

  void decreaseRate();

  long getRate();

  void setRate(long permitsPerSecond);
}
