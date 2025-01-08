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

  private final double minRate;
  private final double maxRate;
  private final double increaseStep;
  private final double decreaseRatio;

  public BulkRequestRateLimiter(FlintOptions flintOptions) {
    // TODO: instead of leaving rateLimiter as null, use a default no-op impl for BulkRequestRateLimiter

    // TODO: validate values?
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    decreaseRatio = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();

    if (flintOptions.getBulkRequestRateLimitPerNodeEnabled()) {
      LOG.info("Setting rate limit for bulk request to " + minRate + " documents/sec");
      this.rateLimiter = RateLimiter.create(minRate);
      MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, (long) minRate);
    } else {
      LOG.info("Rate limit for bulk request was not set.");
    }
  }

  // Wait so it won't exceed rate limit. Does nothing if rate limit is not set.
  public void acquirePermit() {
    if (rateLimiter != null) {
      this.rateLimiter.acquire();
    }
  }

  public void acquirePermit(int permits) {
    if (rateLimiter != null) {
      this.rateLimiter.acquire(permits);
    }
  }

  public double getRate() {
    if (rateLimiter != null) {
      return this.rateLimiter.getRate();
    }
    return 0;
  }

  public void setRate(double permitsPerSecond) {
    if (rateLimiter != null) {
      permitsPerSecond = Math.max(minRate, Math.min(maxRate, permitsPerSecond));
      LOG.info("Setting rate limit for bulk request to " + permitsPerSecond + " documents/sec");
      this.rateLimiter.setRate(permitsPerSecond);
      // TODO: now it's using long metric
      MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, (long) permitsPerSecond);
    }
  }

  public void increaseRate() {
    if (rateLimiter != null) {
      setRate(getRate() + increaseStep);
    }
  }

  public void decreaseRate() {
    if (rateLimiter != null) {
      setRate(getRate() * decreaseRatio);
    }
  }
}
