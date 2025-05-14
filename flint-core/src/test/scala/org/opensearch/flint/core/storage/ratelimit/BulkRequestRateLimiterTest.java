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

class BulkRequestRateLimiterTest {

  private long currentTimeMillis = 0;

  FlintOptions options = new FlintOptions(Map.of(
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
      FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "2000",
      FlintOptions.BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, "20000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, "1000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO, "0.5"));

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

  @Test
  public void increaseRateLimitOnFeedbackNoRetryable() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.acquirePermit(100000);
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(10000));
    assertEquals(3000, rateLimiter.getRate());
  }

  @Test
  public void increaseRateLimitStabilized() {
    Clock clock = mock(Clock.class);
    currentTimeMillis = 0;
    when(clock.millis()).thenAnswer(inv -> currentTimeMillis);

    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options, clock);

    /*
     time | action
    ------|--------
     0000 | permit 15000
    10000 | attempt increase rate -> fail: 15000/10 < threshold = 2000 * 0.8 = 1600
    11000 | permit 18000
    12000 | attempt increase rate -> success: 18000/10 > threshold
    13000 | permit 5000
    14000 | attempt increase rate -> fail: 23000/10 < new threshold = 3000 * 0.8 = 2400
    15000 | permit 1000
    16000 | attempt increase rate -> success: 24000/10 > threshold
    */

    rateLimiter.acquirePermit(15000);
    currentTimeMillis += 10000;
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(10000));
    assertEquals(2000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(18000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(10000));
    assertEquals(3000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(5000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(10000));
    assertEquals(3000, rateLimiter.getRate());

    currentTimeMillis += 1000;
    rateLimiter.acquirePermit(1000);
    currentTimeMillis += 1000;
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(10000));
    assertEquals(4000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackHasRetryable() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(RetryableFailureRequestFeedback.create(10000));
    assertEquals(5000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackHighLatency() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(SuccessRequestFeedback.create(50000));
    assertEquals(5000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitOnFeedbackTimeout() {
    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options);
    rateLimiter.setRate(10000);
    rateLimiter.adaptToFeedback(TimeoutRequestFeedback.create());
    assertEquals(2000, rateLimiter.getRate());
  }

  @Test
  public void decreaseRateLimitStabilized() {
    Clock clock = mock(Clock.class);
    currentTimeMillis = 0;
    when(clock.millis()).thenAnswer(inv -> currentTimeMillis);

    BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(options, clock);
    rateLimiter.setRate(10000);

    rateLimiter.adaptToFeedback(RetryableFailureRequestFeedback.create(10000));
    assertEquals(5000, rateLimiter.getRate());

    // Decrease rate has cooldown 20000 ms
    currentTimeMillis += 10000;
    rateLimiter.adaptToFeedback(RetryableFailureRequestFeedback.create(10000));
    assertEquals(5000, rateLimiter.getRate());

    currentTimeMillis += 10000;
    rateLimiter.adaptToFeedback(RetryableFailureRequestFeedback.create(10000));
    assertEquals(2500, rateLimiter.getRate());
  }
}
