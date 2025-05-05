/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.flint.core.FlintOptions;

/**
 * Hold shared instance of BulkRequestRateLimiter. This class is introduced to make
 * BulkRequestRateLimiter testable and share single instance.
 */
public class BulkRequestRateLimiterHolder {

  private static BulkRequestRateLimiter instance;

  private BulkRequestRateLimiterHolder() {}

  public synchronized static BulkRequestRateLimiter getBulkRequestRateLimiter(
      FlintOptions flintOptions) {
    if (instance == null) {
      if (flintOptions.getBulkRequestRateLimitPerNodeEnabled()) {
        instance = new BulkRequestRateLimiterImpl(flintOptions);
      } else {
        instance = new BulkRequestRateLimiterNoop();
      }
    }
    return instance;
  }

  @VisibleForTesting
  public synchronized static void reset() {
    instance = null;
  }
}
