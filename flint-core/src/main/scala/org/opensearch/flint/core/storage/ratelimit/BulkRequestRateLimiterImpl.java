/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import com.google.common.util.concurrent.RateLimiter;
import java.util.logging.Logger;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;
import org.opensearch.flint.core.storage.RequestRateMeter;

public class BulkRequestRateLimiterImpl implements BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiterImpl.class.getName());
  private RateLimiter rateLimiter;

  private final long minRate;
  private final long maxRate;
  private final long increaseStep;
  private final double decreaseRatio;
  private final RequestRateMeter requestRateMeter;

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions) {
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    decreaseRatio = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();
    requestRateMeter = new RequestRateMeter();

    LOG.info("Setting rate limit for bulk request to " + minRate + " bytes/sec");
    this.rateLimiter = RateLimiter.create(minRate);
    MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, minRate);
  }

  // Wait so it won't exceed rate limit. Does nothing if rate limit is not set.
  @Override
  public void acquirePermit() {
    this.rateLimiter.acquire();
    LOG.info("Acquired 1 permit");
  }

  @Override
  public void acquirePermit(int permits) {
    this.rateLimiter.acquire(permits);
    LOG.info("Acquired " + permits + " permits");
    requestRateMeter.addDataPoint(System.currentTimeMillis(), permits);
  }

  /**
   * Increase rate limit additively.
   */
  @Override
  public void increaseRate() {
    setRate(getRate() + increaseStep);
    /*
    if (isEstimatedCurrentRateCloseToLimit()) {
      setRate(getRate() + increaseStep);
    } else {
      LOG.info("Rate increase was blocked.");
    }
     */
    LOG.info("Current rate limit for bulk request is " + getRate() + " bytes/sec");
  }

  private boolean isEstimatedCurrentRateCloseToLimit() {
    long currentEstimatedRate = requestRateMeter.getCurrentEstimatedRate();
    LOG.info("Current estimated rate is " + currentEstimatedRate + " bytes/sec");
    return getRate() * 0.8 < currentEstimatedRate;
  }

  /**
   * Decrease rate limit multiplicatively.
   */
  @Override
  public void decreaseRate() {
    setRate((long) (getRate() * decreaseRatio));
  }

  @Override
  public long getRate() {
    return (long) this.rateLimiter.getRate();
  }

  /**
   * Set rate limit to the given value, clamped by minRate and maxRate. Non-positive maxRate means
   * there's no maximum rate restriction, and the rate can be set to any value greater than
   * minRate.
   */
  @Override
  public void setRate(long permitsPerSecond) {
    if (maxRate > 0) {
      permitsPerSecond = Math.min(permitsPerSecond, maxRate);
    }
    permitsPerSecond = Math.max(minRate, permitsPerSecond);
    LOG.info("Setting rate limit for bulk request to " + permitsPerSecond + " bytes/sec");
    this.rateLimiter.setRate(permitsPerSecond);
    MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, permitsPerSecond);
  }
}
