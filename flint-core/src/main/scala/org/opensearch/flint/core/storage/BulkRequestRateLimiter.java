/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.google.common.util.concurrent.RateLimiter;
import java.util.logging.Logger;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;

public class BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiter.class.getName());
  private RateLimiter rateLimiter;

  private final long minRate;
  private final long maxRate;
  private final long increaseStep;
  private final double decreaseRatio;

  public BulkRequestRateLimiter(FlintOptions flintOptions) {
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    decreaseRatio = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();

    if (flintOptions.getBulkRequestRateLimitPerNodeEnabled()) {
      LOG.info("Setting rate limit for bulk request to " + minRate + " documents/sec");
      this.rateLimiter = RateLimiter.create(minRate);
      MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, minRate);
    } else {
      LOG.info("Rate limit for bulk request was not set.");
    }
  }

  // Wait so it won't exceed rate limit. Does nothing if rate limit is not set.
  public void acquirePermit() {
    if (rateLimiter != null) {
      this.rateLimiter.acquire();
      LOG.info("Acquired 1 permit");
    }
  }

  public void acquirePermit(int permits) {
    if (rateLimiter != null) {
      this.rateLimiter.acquire(permits);
      LOG.info("Acquired " + permits + " permits");
    }
  }

  /**
   * Increase rate limit additively.
   */
  public void increaseRate() {
    if (rateLimiter != null) {
      setRate(getRate() + increaseStep);
    }
  }

  /**
   * Decrease rate limit multiplicatively.
   */
  public void decreaseRate() {
    if (rateLimiter != null) {
      setRate((long) (getRate() * decreaseRatio));
    }
  }

  /**
   * Rate getter and setter are public for testing purpose
   */
  public long getRate() {
    if (rateLimiter != null) {
      return (long) this.rateLimiter.getRate();
    }
    return 0;
  }

  public void setRate(long permitsPerSecond) {
    if (rateLimiter != null) {
      if (maxRate > 0) {
        permitsPerSecond = Math.min(permitsPerSecond, maxRate);
      }
      permitsPerSecond = Math.max(minRate, permitsPerSecond);
      LOG.info("Setting rate limit for bulk request to " + permitsPerSecond + " documents/sec");
      this.rateLimiter.setRate(permitsPerSecond);
      MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, permitsPerSecond);
    }
  }
}
