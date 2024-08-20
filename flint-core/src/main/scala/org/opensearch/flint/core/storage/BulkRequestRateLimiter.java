package org.opensearch.flint.core.storage;

import dev.failsafe.RateLimiter;
import java.time.Duration;
import java.util.logging.Logger;
import org.opensearch.flint.core.FlintOptions;

public class BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiter.class.getName());
  private RateLimiter<Void> rateLimiter;

  public BulkRequestRateLimiter(FlintOptions flintOptions) {
    long bulkRequestRateLimitPerNode = flintOptions.getBulkRequestRateLimitPerNode();
    if (bulkRequestRateLimitPerNode > 0) {
      LOG.info("Setting rate limit for bulk request to " + bulkRequestRateLimitPerNode + "/sec");
      this.rateLimiter = RateLimiter.<Void>smoothBuilder(
          flintOptions.getBulkRequestRateLimitPerNode(),
          Duration.ofSeconds(1)).build();
    } else {
      LOG.info("Rate limit for bulk request was not set.");
    }
  }

  // Wait so it won't exceed rate limit. Does nothing if rate limit is not set.
  public void acquirePermit() throws InterruptedException {
    if (rateLimiter != null) {
      this.rateLimiter.acquirePermit();
    }
  }
}
