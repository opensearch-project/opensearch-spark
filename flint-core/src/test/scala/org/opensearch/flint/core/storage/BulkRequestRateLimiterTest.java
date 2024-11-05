/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

class BulkRequestRateLimiterTest {
  FlintOptions flintOptionsWithRateLimit = new FlintOptions(ImmutableMap.of(FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE, "1"));
  FlintOptions flintOptionsWithoutRateLimit = new FlintOptions(ImmutableMap.of(FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE, "0"));

  @Test
  void acquirePermitWithRateConfig() throws Exception {
    BulkRequestRateLimiter limiter = new BulkRequestRateLimiter(flintOptionsWithRateLimit);

    assertTrue(timer(() -> {
      limiter.acquirePermit();
      limiter.acquirePermit();
    }) >= 1000);
  }

  @Test
  void acquirePermitWithoutRateConfig() throws Exception {
    BulkRequestRateLimiter limiter = new BulkRequestRateLimiter(flintOptionsWithoutRateLimit);

    assertTrue(timer(() -> {
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
