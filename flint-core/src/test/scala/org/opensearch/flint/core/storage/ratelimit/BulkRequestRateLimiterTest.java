/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

/**
 * TODO: test for adaptToFeedback
 */
class BulkRequestRateLimiterTest {

  private long currentTimeMillis = 0;

  FlintOptions options = new FlintOptions(Map.of(
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
      FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "2000",
      FlintOptions.BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, "20000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_RATE_THRESHOLD, "0",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, "1000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_LATENCY_THRESHOLD, "25000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_FAILURE, "0.9",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_LATENCY, "0.7",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_TIMEOUT, "0.5"));

  FlintOptions optionsWithStabilize = new FlintOptions(Map.of(
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
      FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "2000",
      FlintOptions.BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, "20000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_RATE_THRESHOLD, "0.8",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, "1000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_LATENCY_THRESHOLD, "25000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_FAILURE, "0.9",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_LATENCY, "0.7",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_TIMEOUT, "0.5"));

  @Test
  public void initialRateLimitIsMinRate() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    assertEquals(2000, rateLimiter.getRate());
  }

  @Test
  public void rateLimitBoundedByMaxRate() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(100000);
    assertEquals(20000, rateLimiter.getRate());
  }

  @Test
  public void rateLimitBoundedByMinRate() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(100);
    assertEquals(2000, rateLimiter.getRate());
  }

  // TODO: test acquirePermit without testing internal rateLimiter behavior

  @Test
  public void increaseRateLimitOnFeedbackNoRetryable() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(10000));
    assertEquals(3000, rateLimiter.getRate());
  }

  @Test
  public void increaseRateLimitStabilized() {
    Clock clock = mock(Clock.class);
    currentTimeMillis = 0;
    when(clock.millis()).thenAnswer(inv -> currentTimeMillis);

    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(optionsWithStabilize, clock);

    /*
     time | action
    ------|--------
     0000 | permit 2000
     1000 |
     2000 |
     3000 | attempt increase rate -> fail: 2000/3 < threshold = 2000 * 0.8 = 1600
     4000 | permit 5000
     5000 | attempt increase rate -> success: 5000/3 > threshold
     6000 | permit 2000
     7000 | attempt increase rate -> fail: 7000/3 < new threshold = 3000 * 0.8 = 2400
     8000 | permit 6000
     9000 | attempt increase rate -> success: 8000/3 > threshold
    */

    rateLimiter.acquirePermit(2000);
    currentTimeMillis += 3000;
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(10000));
    assertEquals(2000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(5000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(10000));
    assertEquals(3000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(2000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(10000));
    assertEquals(3000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(6000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(10000));
    assertEquals(4000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackHasRetryable() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(RequestFeedback.hasRetryable(10000));
    assertEquals(9000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackHighLatency() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(RequestFeedback.noRetryable(50000));
    assertEquals(7000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackTimeout() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(RequestFeedback.timeout());
    assertEquals(5000, rateLimiter.getRate());
  }
}
