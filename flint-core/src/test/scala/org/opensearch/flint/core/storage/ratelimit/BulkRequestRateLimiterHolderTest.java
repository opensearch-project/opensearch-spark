/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

class BulkRequestRateLimiterHolderTest {
  FlintOptions flintOptions = new FlintOptions(ImmutableMap.of());

  @BeforeEach
  public void setup() throws Exception {
    BulkRequestRateLimiterHolder.reset();
  }

  @Test
  public void getBulkRequestRateLimiterSingleInstance() {
    BulkRequestRateLimiter instance0 = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(flintOptions);
    BulkRequestRateLimiter instance1 = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(flintOptions);

    assertNotNull(instance0);
    assertEquals(instance0, instance1);
  }

  @Test
  public void getBulkRequestRateLimiterDisabled() {
    BulkRequestRateLimiter rateLimiter = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(flintOptions);
    assertTrue(rateLimiter instanceof BulkRequestRateLimiterNoop);
  }

  @Test
  public void getBulkRequestRateLimiterEnabled() {
    FlintOptions rateEnabledOptions = new FlintOptions(ImmutableMap.of(
        FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true"));
    BulkRequestRateLimiter rateLimiter = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(rateEnabledOptions);
    assertTrue(rateLimiter instanceof BulkRequestRateLimiterImpl);
  }
}