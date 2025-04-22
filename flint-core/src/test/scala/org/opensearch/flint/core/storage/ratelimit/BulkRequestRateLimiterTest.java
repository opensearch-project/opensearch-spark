/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage.ratelimit;


import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.flint.core.FlintOptions;

/**
 * TODO: test for adaptToFeedback
 */
class BulkRequestRateLimiterTest {

  /*
  // Moved from OpenSearchBulkWrapperTest
  // TODO: adjust to test with Feedback rather than mocked response
  // TODO: add tests for stabilize threshold, latency threshold, different decrease scenario
  FlintOptions optionsWithRateLimit = new FlintOptions(Map.of(
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
      FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "2000",
      FlintOptions.BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, "20000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_RATE_THRESHOLD, "0",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, "1000",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO_FAILURE, "0.5"));

  @Test
  public void increaseRateLimitWhenCallSucceed() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(optionsWithRateLimit);
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(bulkRequest, options)).thenReturn(successResponse);
      when(successResponse.hasFailures()).thenReturn(false);

      assertEquals(2000, rateLimiter.getRate());

      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(3000, rateLimiter.getRate());

      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(4000, rateLimiter.getRate());

      // Should not exceed max rate limit
      rateLimiter.setRate(20000);
      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(20000, rateLimiter.getRate());
    });
  }

  @Test
  public void adjustRateLimitWithRetryWhenCallFailOnce() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(optionsWithRateLimit);
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse)
          .thenReturn(successResponse);
      mockFailureResponse();
      when(successResponse.hasFailures()).thenReturn(false);

      rateLimiter.setRate(10000);

      bulkWrapper.bulk(client, bulkRequest, options);

      // Should decrease once then increase once
      assertEquals(6000, rateLimiter.getRate());
    });
  }

  @Test
  public void decreaseRateLimitWhenAllCallFail() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(optionsWithRateLimit);
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse)
          .thenReturn(retriedResponse);
      mockFailureResponse();
      mockRetriedResponse();

      rateLimiter.setRate(20000);

      bulkWrapper.bulk(client, bulkRequest, options);

      // Should decrease three times
      assertEquals(2500, rateLimiter.getRate());
    });
  }

   */

  private interface Procedure {
    void run() throws Exception;
  }

  private long timer(Procedure procedure) throws Exception {
    long start = System.currentTimeMillis();
    procedure.run();
    long end = System.currentTimeMillis();
    return end - start;
  }
}
