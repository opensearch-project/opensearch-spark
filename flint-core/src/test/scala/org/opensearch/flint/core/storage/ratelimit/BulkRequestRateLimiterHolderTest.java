/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

class BulkRequestRateLimiterHolderTest {
  FlintOptions flintOptions = new FlintOptions(ImmutableMap.of());
  @Test
  public void getBulkRequestRateLimiter() {
    BulkRequestRateLimiter instance0 = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(flintOptions);
    BulkRequestRateLimiter instance1 = BulkRequestRateLimiterHolder.getBulkRequestRateLimiter(flintOptions);

    assertNotNull(instance0);
    assertEquals(instance0, instance1);
  }
}