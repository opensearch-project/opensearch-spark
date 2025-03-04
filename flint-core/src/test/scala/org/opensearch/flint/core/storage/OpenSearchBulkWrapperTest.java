/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsTestUtil;
import org.opensearch.rest.RestStatus;

@ExtendWith(MockitoExtension.class)
class OpenSearchBulkWrapperTest {

  private static final long ESTIMATED_SIZE_IN_BYTES = 1000L;
  @Mock
  BulkRequest bulkRequest;
  @Mock
  RequestOptions options;
  @Mock
  BulkResponse successResponse;
  @Mock
  BulkResponse failureResponse;
  @Mock
  BulkResponse retriedResponse;
  @Mock
  BulkResponse conflictResponse;
  @Mock
  RestHighLevelClient client;
  @Mock
  DocWriteResponse docWriteResponse;
  @Mock
  IndexRequest indexRequest0, indexRequest1;

  BulkItemResponse successItem = new BulkItemResponse(0, OpType.CREATE, docWriteResponse);
  BulkItemResponse failureItem = new BulkItemResponse(0, OpType.CREATE,
      new Failure("index", "id", null,
          RestStatus.TOO_MANY_REQUESTS));
  BulkItemResponse conflictItem = new BulkItemResponse(0, OpType.CREATE,
      new Failure("index", "id", null,
          RestStatus.CONFLICT));

  FlintRetryOptions retryOptionsWithRetry = new FlintRetryOptions(Map.of("retry.max_retries", "2"));
  FlintRetryOptions retryOptionsWithoutRetry = new FlintRetryOptions(
      Map.of("retry.max_retries", "0"));

  FlintOptions optionsWithRateLimit = new FlintOptions(Map.of(
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_ENABLED, "true",
      FlintOptions.BULK_REQUEST_MIN_RATE_LIMIT_PER_NODE, "2",
      FlintOptions.BULK_REQUEST_MAX_RATE_LIMIT_PER_NODE, "20",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_INCREASE_STEP, "1",
      FlintOptions.BULK_REQUEST_RATE_LIMIT_PER_NODE_DECREASE_RATIO, "0.5"));

  @Test
  public void withRetryWhenCallSucceed() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(bulkRequest, options)).thenReturn(successResponse);
      when(successResponse.hasFailures()).thenReturn(false);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(successResponse, response);
      verify(client).bulk(bulkRequest, options);

      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);
    });
  }

  @Test
  public void withRetryWhenCallConflict() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(any(), eq(options)))
          .thenReturn(conflictResponse);
      mockConflictResponse();
      when(conflictResponse.hasFailures()).thenReturn(true);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(conflictResponse, response);
      verify(client).bulk(bulkRequest, options);

      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);
    });
  }

  @Test
  public void withRetryWhenCallFailOnce() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse)
          .thenReturn(successResponse);
      mockFailureResponse();
      when(successResponse.hasFailures()).thenReturn(false);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(successResponse, response);
      verify(client, times(2)).bulk(any(), eq(options));
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 1);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);
    });
  }

  @Test
  public void withRetryWhenAllCallFail() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);
      mockFailureResponse();

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(failureResponse, response);
      verify(client, times(3)).bulk(any(), eq(options));
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 2);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC, 1);
    });
  }

  @Test
  public void withRetryWhenCallThrowsShouldNotRetry() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(bulkRequest, options)).thenThrow(new RuntimeException("test"));
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      assertThrows(RuntimeException.class,
          () -> bulkWrapper.bulk(client, bulkRequest, options));

      verify(client).bulk(bulkRequest, options);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);
    });
  }

  @Test
  public void withoutRetryWhenCallFail() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterNoop();
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithoutRetry, rateLimiter);
      when(client.bulk(bulkRequest, options))
          .thenReturn(failureResponse);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);
      mockFailureResponse();

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(failureResponse, response);
      verify(client).bulk(bulkRequest, options);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);
    });
  }

  @Test
  public void increaseRateLimitWhenCallSucceed() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      BulkRequestRateLimiter rateLimiter = new BulkRequestRateLimiterImpl(optionsWithRateLimit);
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter);
      when(client.bulk(bulkRequest, options)).thenReturn(successResponse);
      when(successResponse.hasFailures()).thenReturn(false);
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      assertEquals(2, rateLimiter.getRate());

      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(3, rateLimiter.getRate());

      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(4, rateLimiter.getRate());

      // Should not exceed max rate limit
      rateLimiter.setRate(20);
      bulkWrapper.bulk(client, bulkRequest, options);
      assertEquals(20, rateLimiter.getRate());
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
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);

      rateLimiter.setRate(10);

      bulkWrapper.bulk(client, bulkRequest, options);

      // Should decrease once then increase once
      assertEquals(6, rateLimiter.getRate());
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
      when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));
      when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);
      mockFailureResponse();
      mockRetriedResponse();

      rateLimiter.setRate(20);

      bulkWrapper.bulk(client, bulkRequest, options);

      // Should decrease three times
      assertEquals(2.5, rateLimiter.getRate());
    });
  }

  private void mockFailureResponse() {
    when(failureResponse.hasFailures()).thenReturn(true);
    when(failureResponse.getItems()).thenReturn(new BulkItemResponse[]{successItem, failureItem});
  }

  private void mockConflictResponse() {
    when(conflictResponse.hasFailures()).thenReturn(true);
    when(conflictResponse.getItems()).thenReturn(new BulkItemResponse[]{successItem, conflictItem});
  }
  private void mockRetriedResponse() {
    when(retriedResponse.hasFailures()).thenReturn(true);
    when(retriedResponse.getItems()).thenReturn(new BulkItemResponse[]{failureItem});
  }
}
