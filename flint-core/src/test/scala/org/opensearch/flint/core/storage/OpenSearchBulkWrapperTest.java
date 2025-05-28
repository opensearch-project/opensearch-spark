/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsTestUtil;
import org.opensearch.flint.core.storage.ratelimit.BulkRequestRateLimiter;
import org.opensearch.flint.core.storage.ratelimit.RequestFeedback;
import org.opensearch.flint.core.storage.ratelimit.RetryableFailureRequestFeedback;
import org.opensearch.flint.core.storage.ratelimit.SuccessRequestFeedback;
import org.opensearch.flint.core.storage.ratelimit.TimeoutRequestFeedback;
import org.opensearch.rest.RestStatus;

@ExtendWith(MockitoExtension.class)
class OpenSearchBulkWrapperTest {

  private static final long ESTIMATED_SIZE_IN_BYTES = 1000L;
  private final long CLOCK_INTERVAL = 1000L;
  @Mock
  BulkRequest bulkRequest;
  @Mock
  RequestOptions options;
  @Mock
  BulkResponse successResponse;
  @Mock
  BulkResponse failureResponse;
  @Mock
  BulkResponse conflictResponse;
  @Mock
  RestHighLevelClient client;
  @Mock
  DocWriteResponse docWriteResponse;
  @Mock
  IndexRequest indexRequest0, indexRequest1;
  @Mock
  BulkRequestRateLimiter rateLimiter;
  @Mock
  Clock clock;

  @Captor
  ArgumentCaptor<RequestFeedback> feedbackCaptor;

  BulkItemResponse successItem = new BulkItemResponse(0, OpType.CREATE, docWriteResponse);
  BulkItemResponse failureItem = new BulkItemResponse(0, OpType.CREATE,
      new Failure("index", "id", null,
          RestStatus.TOO_MANY_REQUESTS));
  BulkItemResponse conflictItem = new BulkItemResponse(0, OpType.CREATE,
      new Failure("index", "id", null,
          RestStatus.CONFLICT));

  FlintRetryOptions retryOptionsWithRetry = new FlintRetryOptions(Map.of("retry.bulk.max_retries", "2"));
  FlintRetryOptions retryOptionsWithoutRetry = new FlintRetryOptions(
      Map.of("retry.bulk.max_retries", "0"));

  @BeforeEach
  public void setup() {
    when(bulkRequest.estimatedSizeInBytes()).thenReturn(ESTIMATED_SIZE_IN_BYTES);
    // Conditional stub based on bulk retry occurrence
    lenient().when(bulkRequest.requests()).thenReturn(ImmutableList.of(indexRequest0, indexRequest1));

    // Mock clock with fixed interval between each call
    AtomicLong counter = new AtomicLong(0);
    when(clock.millis()).thenAnswer(inv -> counter.getAndAdd(CLOCK_INTERVAL));
  }

  @Test
  public void withRetryWhenCallSucceed() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(bulkRequest, options)).thenReturn(successResponse);
      when(successResponse.hasFailures()).thenReturn(false);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(successResponse, response);
      verify(client).bulk(bulkRequest, options);

      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter).adaptToFeedback(feedbackCaptor.capture());
      assertSuccessRequestFeedback(feedbackCaptor.getValue(), 1000);
    });
  }

  @Test
  public void withRetryWhenCallConflict() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(any(), eq(options)))
          .thenReturn(conflictResponse);
      mockConflictResponse();
      when(conflictResponse.hasFailures()).thenReturn(true);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(conflictResponse, response);
      verify(client).bulk(bulkRequest, options);

      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter).adaptToFeedback(feedbackCaptor.capture());
      assertSuccessRequestFeedback(feedbackCaptor.getValue(), 1000);
    });
  }

  @Test
  public void withRetryWhenCallFailOnce() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse)
          .thenReturn(successResponse);
      mockFailureResponse();
      when(successResponse.hasFailures()).thenReturn(false);

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(successResponse, response);
      verify(client, times(2)).bulk(any(), eq(options));
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 1);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter, times(2)).adaptToFeedback(feedbackCaptor.capture());
      List<RequestFeedback> capturedFeedbacks = feedbackCaptor.getAllValues();
      assertRetryableFailureRequestFeedback(capturedFeedbacks.get(0), 1000);
      assertSuccessRequestFeedback(capturedFeedbacks.get(1), 1000);
    });
  }

  @Test
  public void withRetryWhenAllCallFail() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(any(), eq(options)))
          .thenReturn(failureResponse);
      mockFailureResponse();

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(failureResponse, response);
      verify(client, times(3)).bulk(any(), eq(options));
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 2);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC, 1);

      verify(rateLimiter, times(3)).adaptToFeedback(feedbackCaptor.capture());
      List<RequestFeedback> capturedFeedbacks = feedbackCaptor.getAllValues();
      capturedFeedbacks.forEach(feedback -> assertRetryableFailureRequestFeedback(feedback, 1000));
    });
  }

  @Test
  public void withRetryWhenCallThrowsTimeoutShouldNotRetry() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(bulkRequest, options)).thenThrow(new java.net.SocketTimeoutException("test"));

      // Bulk wrapper unwraps original exception and throws RuntimeException
      assertThrows(RuntimeException.class,
          () -> bulkWrapper.bulk(client, bulkRequest, options));

      verify(client).bulk(bulkRequest, options);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter).adaptToFeedback(feedbackCaptor.capture());
      assertTimeoutRequestFeedback(feedbackCaptor.getValue());
    });
  }

  @Test
  public void withRetryWhenCallThrowsOtherShouldNotRetryAndAdaptRate() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithRetry, rateLimiter, clock);
      when(client.bulk(bulkRequest, options)).thenThrow(new RuntimeException("test"));

      assertThrows(RuntimeException.class,
          () -> bulkWrapper.bulk(client, bulkRequest, options));

      verify(client).bulk(bulkRequest, options);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter, times(0)).adaptToFeedback(any());
    });
  }

  @Test
  public void withoutRetryWhenCallFail() throws Exception {
    MetricsTestUtil.withMetricEnv(verifier -> {
      OpenSearchBulkWrapper bulkWrapper = new OpenSearchBulkWrapper(
          retryOptionsWithoutRetry, rateLimiter, clock);
      when(client.bulk(bulkRequest, options))
          .thenReturn(failureResponse);
      mockFailureResponse();

      BulkResponse response = bulkWrapper.bulk(client, bulkRequest, options);

      assertEquals(failureResponse, response);
      verify(client).bulk(bulkRequest, options);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, ESTIMATED_SIZE_IN_BYTES);
      verifier.assertHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, 0);
      verifier.assertMetricNotExist(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC);

      verify(rateLimiter).adaptToFeedback(feedbackCaptor.capture());
      assertRetryableFailureRequestFeedback(feedbackCaptor.getValue(), 1000);
    });
  }

  private void assertSuccessRequestFeedback(RequestFeedback feedback, long expectedLatency) {
    assertNotNull(feedback, "Captured feedback should not be null");
    assertTrue(feedback instanceof SuccessRequestFeedback, "Feedback should be an instance of SuccessRequestFeedback. Was: " + feedback.getClass().getName());
    SuccessRequestFeedback successRequestFeedback = (SuccessRequestFeedback) feedback;
    assertEquals(expectedLatency, successRequestFeedback.latencyMillis, "Latency value should match the expected latency");
  }

  private void assertRetryableFailureRequestFeedback(RequestFeedback feedback, long expectedLatency) {
    assertNotNull(feedback, "Captured feedback should not be null");
    assertTrue(feedback instanceof RetryableFailureRequestFeedback, "Feedback should be an instance of RetryableFailureRequestFeedback. Was: " + feedback.getClass().getName());
    RetryableFailureRequestFeedback retryableFailureRequestFeedback = (RetryableFailureRequestFeedback) feedback;
    assertEquals(expectedLatency, retryableFailureRequestFeedback.latencyMillis, "Latency value should match the expected latency");
  }

  private void assertTimeoutRequestFeedback(RequestFeedback feedback) {
    assertNotNull(feedback, "Captured feedback should not be null");
    assertTrue(feedback instanceof TimeoutRequestFeedback, "Feedback should be an instance of TimeoutRequestFeedback. Was: " + feedback.getClass().getName());
  }

  private void mockFailureResponse() {
    when(failureResponse.hasFailures()).thenReturn(true);
    when(failureResponse.getItems()).thenReturn(new BulkItemResponse[]{successItem, failureItem});
  }

  private void mockConflictResponse() {
    when(conflictResponse.hasFailures()).thenReturn(true);
    when(conflictResponse.getItems()).thenReturn(new BulkItemResponse[]{successItem, conflictItem});
  }
}
