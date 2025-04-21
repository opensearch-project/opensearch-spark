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
  private final double increaseRateThreshold;
  private final long increaseStep;
  private final double decreaseRatio;
  private final long latencyThreshold;
  private final double decreaseRatioLatency;
  private final double decreaseRatioFailure;
  private final double decreaseRatioTimeout;
  private final RequestRateMeter requestRateMeter;

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions) {
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseRateThreshold = 0.8f; // TODO. Set to 0 to disable stabilizing
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    latencyThreshold = 25000; // TODO: this is millisecond
    decreaseRatio = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();
    decreaseRatioLatency = 0.7f; // TODO
    decreaseRatioFailure = 0.9f; // TODO
    decreaseRatioTimeout = 0.5f; // TODO
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

  @Override
  public long getRate() {
    return (long) this.rateLimiter.getRate();
  }

  @Override
  public void adaptToFeedback(RequestFeedback feedback) {
    if (feedback.isSuccess) {
      if (feedback.latency > latencyThreshold) {
        LOG.warning("Decreasing rate. Reason: Bulk latency high. Latency = " + feedback.latency + " exceeds threshold " + latencyThreshold);
        decreaseRate(decreaseRatioLatency);
      } else {
        increaseRate();
      }
    } else {
      if (feedback.isTimeout) {
        LOG.warning("Decreasing rate. Reason: Bulk request socket/connection timeout.");
        decreaseRate(decreaseRatioTimeout);
      } else {
        LOG.warning("Decreasing rate. Reason: Bulk request failed.");
        decreaseRate(decreaseRatioFailure);
      }
    }
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

  /**
   * Increase rate limit additively.
   */
  private void increaseRate() {
    setRate(getRate() + increaseStep);
    if (isEstimatedCurrentRateCloseToLimit()) {
      setRate(getRate() + increaseStep);
    } else {
      LOG.warning("Rate increase blocked. Reason: Current rate " + requestRateMeter.getCurrentEstimatedRate() + " is not close to limit " + getRate());
    }
  }

  private boolean isEstimatedCurrentRateCloseToLimit() {
    long currentEstimatedRate = requestRateMeter.getCurrentEstimatedRate();
    LOG.warning("Current estimated rate is " + currentEstimatedRate + " bytes/sec");
    return getRate() * increaseRateThreshold < currentEstimatedRate;
  }

  /**
   * Decrease rate limit multiplicatively.
   */
  private void decreaseRate(double decreaseRatio) {
    setRate((long) (getRate() * decreaseRatio));
  }
}
