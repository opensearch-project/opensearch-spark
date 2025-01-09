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
  private final double partialFailureThreshold;

  public BulkRequestRateLimiter(FlintOptions flintOptions) {
    // TODO: instead of leaving rateLimiter as null, use a default no-op impl for BulkRequestRateLimiter?

    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    decreaseRatio = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();
    partialFailureThreshold = flintOptions.getBulkRequestRateLimitPerNodePartialFailureThreshold();

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
   * Notify rate limiter of the failure rate of a bulk request. Additive-increase or multiplicative-decrease
   * rate limit based on the failure rate. Does nothing if rate limit is not set.
   * @param failureRate failure rate of the bulk request between 0 and 1
   */
  public void reportFailure(double failureRate) {
    if (rateLimiter != null) {
      if (failureRate > partialFailureThreshold) {
        decreaseRate();
      } else {
        increaseRate();
      }
    }
  }

  /**
   * Rate getter and setter are public for test purpose only
   */
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

  private void increaseRate() {
    if (rateLimiter != null) {
      setRate(getRate() + increaseStep);
    }
  }

  private void decreaseRate() {
    if (rateLimiter != null) {
      setRate(getRate() * decreaseRatio);
    }
  }
}
