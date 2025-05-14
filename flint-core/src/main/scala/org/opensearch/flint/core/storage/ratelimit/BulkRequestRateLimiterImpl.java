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

/**
 * An adaptive rate limiter implementation for bulk requests to OpenSearch that responds to multiple
 * performance signals. This implementation extends Guava's RateLimiter with adaptive rate adjustment
 * based on request performance feedback.
 *
 * The rate limiter increases rate additively when current throughput is close to the limit
 * and decreases rate multiplicatively when encountering issues (high latency, failures, timeouts).
 *
 * The rate limiter has two main operations:
 * - {@code acquirePermit}: Blocks until permission is granted to proceed with the request
 * - {@code adaptToFeedback}: Reports request outcome to adjust the rate limit dynamically
 *
 * Key features:
 * - Rate measured in bytes/second
 * - Multi-signal feedback system responding to:
 *   - Request latency (reduces rate when latency exceeds threshold)
 *   - Request timeout (reduces rate to minimum)
 *   - Request failure
 *   - Request success
 * - Stabilized rate adjustment mechanism:
 *   - Monitor current request rate and increase rate only if current throughput is close to the limit
 *   - Implement cooldown period between rate decreases to prevent rapid exponential decline
 *     when multiple requests report bad signals simultaneously
 *
 * Note: {@code acquirePermit} is a blocking operation. The thread will wait until the rate limiter
 * grants permission based on the current rate limit. You don't need to check any other conditions.
 * Just call acquirePermit and proceed when it returns.
 */
public class BulkRequestRateLimiterImpl implements BulkRequestRateLimiter, FeedbackHandler {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiterImpl.class.getName());

  /** Minimum rate limit. */
  private final long minRate;

  /** Maximum rate limit. */
  private final long maxRate;

  /** Additive rate limit increase step. */
  private final long increaseStep;

  /** Rate limit decrease multiplier for high latency request. */
  private final double decreaseRatioLatency;

  /** Rate limit decrease multiplier for request failure. */
  private final double decreaseRatioFailure;

  /**
   * Threshold for stabilizing rate limit increase. Only increase rate limit when current rate
   * is at least 80% of rate limit.
   */
  private final double increaseRateThreshold = 0.8;

  /**
   * Rate limit decrease multiplier for timeout event.
   * Setting this to 0 will reset the rate limit to minRate when timeout occurs.
   */
  private final double decreaseRatioTimeout = 0;

  /** Latency threshold to decide whether a request is high latency. */
  private final long latencyThresholdMillis = 20000;

  /** Cooldown time for decreasing rate limit. Rate limit will not decrease during cooldown. */
  private final long decreaseCooldownMillis = 20000;

  private final Clock clock;
  private final RequestRateMeter requestRateMeter;
  private final RateLimiter rateLimiter;

  /** A timestamp for the cooldown mechanism. Decrease is only allowed after this timestamp. */
  private long allowDecreaseAfter = 0;

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions) {
    this(flintOptions, Clock.systemUTC());
  }

  public BulkRequestRateLimiterImpl(FlintOptions flintOptions, Clock clock) {
    minRate = flintOptions.getBulkRequestMinRateLimitPerNode();
    maxRate = flintOptions.getBulkRequestMaxRateLimitPerNode();
    increaseStep = flintOptions.getBulkRequestRateLimitPerNodeIncreaseStep();
    decreaseRatioFailure = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();
    decreaseRatioLatency = flintOptions.getBulkRequestRateLimitPerNodeDecreaseRatio();

    this.clock = clock;
    this.requestRateMeter = new RequestRateMeter(clock);
    this.rateLimiter = RateLimiter.create(minRate);
    MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RATE_LIMIT_METRIC, minRate);
  }

  /**
   * Acquires a single permit from the rate limiter, blocking until the request can be granted.
   */
  @Override
  public void acquirePermit() {
    this.rateLimiter.acquire();
    LOG.info("Acquired 1 permit");
  }

  /**
   * Acquires the specified number of permits from the rate limiter, blocking until granted.
   *
   * @param permits number of permits (request size in bytes) to acquire
   */
  @Override
  public void acquirePermit(int permits) {
    this.rateLimiter.acquire(permits);
    LOG.info("Acquired " + permits + " permits");
    requestRateMeter.addDataPoint(clock.millis(), permits);
  }

  /**
   * Returns the current rate limit.
   * This is primarily used for monitoring and testing purposes.
   */
  @Override
  public long getRate() {
    return (long) this.rateLimiter.getRate();
  }

  /**
   * Adapt rate limit based on multi signal feedback.
   */
  @Override
  public void adaptToFeedback(RequestFeedback feedback) {
    long newRateLimit = feedback.suggestNewRateLimit(this);
    setRate(newRateLimit);
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

  @Override
  public long handleFeedback(TimeoutRequestFeedback feedbackEvent) {
    long currentRateLimit = getRate();
    if (canDecreaseRate()) {
      LOG.warning("Decreasing rate. Reason: Bulk request socket/connection timeout.");
      return (long) (currentRateLimit * decreaseRatioTimeout);
    }
    return currentRateLimit;
  }

  @Override
  public long handleFeedback(RetryableFailureRequestFeedback feedbackEvent) {
    long currentRateLimit = getRate();
    if (canDecreaseRate()) {
      LOG.warning("Decreasing rate. Reason: Bulk request failed.");
      return (long) (currentRateLimit * decreaseRatioFailure);
    }
    return currentRateLimit;
  }

  @Override
  public long handleFeedback(SuccessRequestFeedback feedbackEvent) {
    long currentRateLimit = getRate();
    if (feedbackEvent.latencyMillis > latencyThresholdMillis) {
      if (canDecreaseRate()) {
        LOG.warning("Decreasing rate. Reason: Bulk latency high. Latency = " + feedbackEvent.latencyMillis + " exceeds threshold " + latencyThresholdMillis);
        return (long) (currentRateLimit * decreaseRatioLatency);
      }
      return currentRateLimit;
    }
    if (isEstimatedCurrentRateCloseToLimit()) {
      return currentRateLimit + increaseStep;
    }
    LOG.warning("Rate increase blocked. Reason: Current rate is not close to limit " + currentRateLimit);
    return currentRateLimit;
  }

  /**
   * Estimates the current request rate based on recent request history.
   * Used to determine if rate limit should be increased.
   */
  private long getEstimatedCurrentRate() {
    long currentEstimatedRate = requestRateMeter.getCurrentEstimatedRate();
    LOG.warning("Current estimated rate is " + currentEstimatedRate + " bytes/sec");
    return currentEstimatedRate;
  }

  /**
   * Checks if the current request rate is close enough to the rate limit to justify an increase.
   * Helps prevent unnecessary rate increases when current throughput is well below the limit.
   */
  private boolean isEstimatedCurrentRateCloseToLimit() {
    long currentEstimatedRate = getEstimatedCurrentRate();
    return getRate() * increaseRateThreshold <= currentEstimatedRate;
  }

  private boolean canDecreaseRate() {
    if (clock.millis() >= allowDecreaseAfter) {
      allowDecreaseAfter = clock.millis() + decreaseCooldownMillis;
      return true;
    }
    return false;
  }
}
