/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
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
  private final double decreaseRatioTimeout;
  private final Clock clock;
  private final RequestRateMeter requestRateMeter;
  private final RateLimiter rateLimiter;

  private boolean slowStart = true;
  // TODO: ssthreshold

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
    decreaseRatioTimeout = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatioTimeout();
    this.clock = clock;
    this.requestRateMeter = new RequestRateMeter(clock);

    LOG.info("Setting rate limit for bulk request to " + minRate + " bytes/sec");
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
      LOG.warning("Decreasing rate. Reason: Bulk request socket/connection timeout.");
      decreaseRate(decreaseRatioTimeout);
      slowStart = true;
      return;
    }
    if (feedback.hasRetryableFailure){
      LOG.warning("Decreasing rate. Reason: Bulk request failed.");
      decreaseRate(decreaseRatioFailure);
      slowStart = false;
      return;
    }
    if (feedback.latency > latencyThreshold) {
      LOG.warning("Decreasing rate. Reason: Bulk latency high. Latency = " + feedback.latency + " exceeds threshold " + latencyThreshold);
      decreaseRate(decreaseRatioLatency);
      slowStart = false;
      return;
    }
    increaseRate(feedback.requestSize);
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
  private void increaseRate(int increment) {
    if (!isEstimatedCurrentRateCloseToLimit()) {
      LOG.warning("Rate increase blocked. Reason: Current rate " + requestRateMeter.getCurrentEstimatedRate() + " is not close to limit " + getRate());
      return;
    }
    if (slowStart) {
      setRate(getRate() + increment);
    } else {
      setRate(getRate() + (long) (increaseStep * increment / (double) getRate()));
    }
  }

  private boolean isEstimatedCurrentRateCloseToLimit() {
    long currentEstimatedRate = requestRateMeter.getCurrentEstimatedRate();
    LOG.warning("Current estimated rate is " + currentEstimatedRate + " bytes/sec");
    return getRate() * increaseRateThreshold <= currentEstimatedRate;
  }

  /**
   * Decrease rate limit multiplicatively.
   */
  private void decreaseRate(double decreaseRatio) {
    setRate((long) (getRate() * decreaseRatio));
  }
}
