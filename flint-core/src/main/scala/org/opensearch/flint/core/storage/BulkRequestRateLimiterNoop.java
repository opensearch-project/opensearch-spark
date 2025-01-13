/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.util.logging.Logger;

public class BulkRequestRateLimiterNoop implements BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiterNoop.class.getName());

  public BulkRequestRateLimiterNoop() {
    LOG.info("Rate limit for bulk request was not set.");
  }

  @Override
  public void acquirePermit() {}

  @Override
  public void acquirePermit(int permits) {}

  @Override
  public void increaseRate() {}

  @Override
  public void decreaseRate() {}

  @Override
  public long getRate() {
    return 0;
  }

  @Override
  public void setRate(long permitsPerSecond) {}
}

