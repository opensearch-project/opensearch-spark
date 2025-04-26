/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;
import org.opensearch.flint.core.storage.RequestRateMeter;

public class BulkRequestRateLimiterImpl implements BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiterImpl.class.getName());

  private final long minRate;
  private final long maxRate;
  private final double increaseRateThreshold;
  private final long increaseStep;
  private final long latencyThreshold;
  private final double decreaseRatioLatency;
  private final double decreaseRatioFailure;
  private final double decreaseRatioTimeout = 0; // decrease to minimum
  private final long decreaseCooldown;
  private final Clock clock;
  private final RequestRateMeter requestRateMeter;
  private final RateLimiter rateLimiter;

  // private boolean slowStart = true;
  private long allowDecreaseAfter = 0;
  // private long ssThreshold;

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions) {
    this(flintOptions, Clock.systemUTC());
  }

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions, Clock clock) {
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseRateThreshold = flintOptions.getBulkRequestRateLimitPerNodeIncreaseRateThreshold();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    latencyThreshold = flintOptions.getBulkRequestRateLimitPerNodeLatencyThreshold();
    decreaseRatioFailure = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatioFailure();
    decreaseRatioLatency = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatioLatency();
    decreaseCooldown = latencyThreshold;

    this.clock = clock;
    this.requestRateMeter = new RequestRateMeter(clock);
    // ssThreshold = maxRate;

    this.rateLimiter = RateLimiter.create(minRate);
    MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, minRate);
  }

  @Override
  public void acquirePermit() {
    this.rateLimiter.acquire();
    LOG.info("Acquired 1 permit");
  }

  @Override
  public void acquirePermit(int permits) {
    this.rateLimiter.acquire(permits);
    LOG.info("Acquired " + permits + " permits");
    requestRateMeter.addDataPoint(clock.millis(), permits);
  }

  @Override
  public long getRate() {
    return (long) this.rateLimiter.getRate();
  }

  /**
   * Adapt rate limit based on multi signal feedback.
   * @param feedback
   */
  @Override
  public void adaptToFeedback(RequestFeedback feedback) {
    if (feedback.isTimeout) {
      if (canDecreaseRate()) {
        LOG.warning("Decreasing rate. Reason: Bulk request socket/connection timeout.");
        // slowStart = true;
        decreaseRate(decreaseRatioTimeout);
        // ssThreshold = getCurrentEstimatedRate() / 2;
      }
      return;
    }
    if (feedback.hasRetryableFailure){
      if (canDecreaseRate()) {
        LOG.warning("Decreasing rate. Reason: Bulk request failed.");
        // slowStart = false;
        decreaseRate(decreaseRatioFailure);
      }
      return;
    }
    if (feedback.latency > latencyThreshold) {
      if (canDecreaseRate()) {
        LOG.warning("Decreasing rate. Reason: Bulk latency high. Latency = " + feedback.latency + " exceeds threshold " + latencyThreshold);
        // slowStart = false;
        decreaseRate(decreaseRatioLatency);
      }
      return;
    }
    increaseRate();
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
   * Increase rate limit. TODO
   */
  private void increaseRate() {
    if (!isCurrentEstimatedRateCloseToLimit()) {
      LOG.warning("Rate increase blocked. Reason: Current rate is not close to limit " + getRate());
      return;
    }
    /*
    if (slowStart) {
      setRate((long) (getRate() * 1.5)); // TODO: config: slow start boost
      if (getRate() >= ssThreshold) {
        slowStart = false;
      }
    } else {
      setRate(getRate() + increaseStep);
    }
     */
    setRate(getRate() + increaseStep);
  }

  private long getCurrentEstimatedRate() {
    long currentEstimatedRate = requestRateMeter.getCurrentEstimatedRate();
    LOG.warning("Current estimated rate is " + currentEstimatedRate + " bytes/sec");
    return currentEstimatedRate;
  }

  private boolean isCurrentEstimatedRateCloseToLimit() {
    long currentEstimatedRate = getCurrentEstimatedRate();
    return getRate() * increaseRateThreshold <= currentEstimatedRate;
  }

  private boolean canDecreaseRate() {
    if (clock.millis() > allowDecreaseAfter) {
      allowDecreaseAfter = clock.millis() + decreaseCooldown;
      return true;
    }
    return false;
  }

  /**
   * Decrease rate limit multiplicatively.
   */
  private void decreaseRate(double decreaseRatio) {
    setRate((long) (getRate() * decreaseRatio));
    /*
    if (getRate() == minRate) {
      slowStart = true;
    }
     */
  }
}
