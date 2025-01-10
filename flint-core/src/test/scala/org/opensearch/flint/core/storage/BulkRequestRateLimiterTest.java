/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;


import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

/**
 * These tests are largely dependent on the choice of the underlying rate limiter. While conceptually
 * they all distribute permits at some rate, the actual behavior varies based on implementation.
 * To avoid flakiness and creating test cases for specific implementation, we measure the time required
 * for acquiring several permits, and set lenient thresholds.
 */
class BulkRequestRateLimiterTest {

  @Test
  void acquirePermitWithRateConfig() throws Exception {
    FlintOptions options = new FlintOptions(ImmutableMap.of(
        FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
        FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "1"));
    BulkRequestRateLimiter limiter = new BulkRequestRateLimiterImpl(options);

    assertTrue(timer(() -> {
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
    }) >= 4500);
    assertTrue(timer(() -> {
      limiter.acquirePermit(5);
      limiter.acquirePermit();
    }) >= 4500);
  }

  @Test
  void acquirePermitWithoutRateConfig() throws Exception {
    BulkRequestRateLimiter limiter = new BulkRequestRateLimiterNoop();

    assertTrue(timer(() -> {
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
      limiter.acquirePermit();
    }) < 100);
  }

  private interface Procedure {
    void run() throws Exception;
  }

  private long timer(Procedure procedure) throws Exception {
    long start = System.currentTimeMillis();
    procedure.run();
    long end = System.currentTimeMillis();
    return end - start;
  }
}
