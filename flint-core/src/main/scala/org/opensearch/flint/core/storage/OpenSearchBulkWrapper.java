/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;
import org.opensearch.flint.core.storage.ratelimit.BulkRequestRateLimiter;
import org.opensearch.flint.core.storage.ratelimit.RequestFeedback;
import org.opensearch.rest.RestStatus;

/**
 * Wrapper class for OpenSearch bulk API with retry and rate limiting capability.
 */
public class OpenSearchBulkWrapper {

  private static final Logger LOG = Logger.getLogger(OpenSearchBulkWrapper.class.getName());

  private final RetryPolicy<BulkResponse> retryPolicy;
  private final BulkRequestRateLimiter rateLimiter;
  private final Set<Integer> retryableStatusCodes;

  public OpenSearchBulkWrapper(FlintRetryOptions retryOptions, BulkRequestRateLimiter rateLimiter) {
    this.retryPolicy = retryOptions.getBulkRetryPolicy(bulkItemRetryableResultPredicate);
    this.rateLimiter = rateLimiter;
    this.retryableStatusCodes = retryOptions.getRetryableHttpStatusCodes();
  }

  /**
   * Bulk request with retry and rate limiting. Delegate bulk request to the client, and retry the
   * request if the response contains retryable failure. It won't retry when bulk call thrown
   * exception. In addition, adjust rate limit based on the responses.
   * @param client used to call bulk API
   * @param bulkRequest requests passed to bulk method
   * @param options options passed to bulk method
   * @return Last result
   */
  public BulkResponse bulk(RestHighLevelClient client, BulkRequest bulkRequest, RequestOptions options) {
    return bulkWithPartialRetry(client, bulkRequest, options);
  }

  private BulkResponse bulkWithPartialRetry(RestHighLevelClient client, BulkRequest bulkRequest,
      RequestOptions options) {
    final AtomicInteger requestCount = new AtomicInteger(0);
    try {
      final AtomicReference<BulkRequest> nextRequest = new AtomicReference<>(bulkRequest);
      BulkResponse res = Failsafe
          .with(retryPolicy)
          .onFailure((event) -> {
            if (event.isRetry()) {
              MetricsUtil.addHistoricGauge(
                  MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC, 1);
            }
          })
          .get(() -> {
            requestCount.incrementAndGet();
            // converting to int should be safe because bulk request bytes is restricted by batch_bytes config
            rateLimiter.acquirePermit((int) nextRequest.get().estimatedSizeInBytes());

            try {
              long startTime = System.currentTimeMillis();
              BulkResponse response = client.bulk(nextRequest.get(), options);
              long latency = System.currentTimeMillis() - startTime;

              if (!bulkItemRetryableResultPredicate.test(response)) {
                rateLimiter.adaptToFeedback(RequestFeedback.success(latency));
              } else {
                LOG.info("Bulk request failed. attempt = " + (requestCount.get() - 1));
                rateLimiter.adaptToFeedback(RequestFeedback.failure(latency));
                if (retryPolicy.getConfig().allowsRetries()) {
                  nextRequest.set(getRetryableRequest(nextRequest.get(), response));
                }
              }
              return response;
            } catch (Exception e) {
              if (isTimeoutException(e)) {
                rateLimiter.adaptToFeedback(RequestFeedback.timeout());
              }
              throw e; // let Failsafe retry policy decide
            }
          });
      return res;
    } catch (FailsafeException ex) {
      LOG.severe("Request failed permanently. Re-throwing original exception.");

      // unwrap original exception and throw
      throw new RuntimeException(ex.getCause());
    } finally {
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, bulkRequest.estimatedSizeInBytes());
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, requestCount.get() - 1);
    }
  }

  private boolean isTimeoutException(Exception e) {
    return e instanceof java.net.SocketTimeoutException ||
        e instanceof  java.net.ConnectException ||
        e.getCause() instanceof java.net.SocketTimeoutException ||
        e.getCause() instanceof java.net.ConnectException;
  }

  private BulkRequest getRetryableRequest(BulkRequest request, BulkResponse response) {
    List<DocWriteRequest<?>> bulkItemRequests = request.requests();
    BulkItemResponse[] bulkItemResponses = response.getItems();
    BulkRequest nextRequest = new BulkRequest()
        .setRefreshPolicy(request.getRefreshPolicy());
    nextRequest.setParentTask(request.getParentTask());
    for (int i = 0; i < bulkItemRequests.size(); i++) {
      if (isItemRetryable(bulkItemResponses[i])) {
        verifyIdMatch(bulkItemRequests.get(i), bulkItemResponses[i]);
        nextRequest.add(bulkItemRequests.get(i));
      }
    }
    LOG.info(String.format("Added %d requests to nextRequest", nextRequest.requests().size()));
    return nextRequest;
  }

  private static void verifyIdMatch(DocWriteRequest<?> request, BulkItemResponse response) {
    if (request.id() != null && !request.id().equals(response.getId())) {
      throw new RuntimeException("id doesn't match: " + request.id() + " / " + response.getId());
    }
  }

  /**
   * A predicate to decide if a BulkResponse is retryable or not.
   */
  private final CheckedPredicate<BulkResponse> bulkItemRetryableResultPredicate = bulkResponse ->
      bulkResponse.hasFailures() && isRetryable(bulkResponse);

  private boolean isRetryable(BulkResponse bulkResponse) {
    if (Arrays.stream(bulkResponse.getItems())
        .anyMatch(itemResp -> isItemRetryable(itemResp))) {
      LOG.info("Found retryable failure in the bulk response");
      return true;
    }
    return false;
  }

  private boolean isItemRetryable(BulkItemResponse itemResponse) {
    return itemResponse.isFailed() && !isCreateConflict(itemResponse)
        && isFailureStatusRetryable(itemResponse);
  }

  private static boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE &&
        itemResp.getFailure().getStatus() == RestStatus.CONFLICT;
  }

  private boolean isFailureStatusRetryable(BulkItemResponse itemResp) {
    if (retryableStatusCodes.contains(itemResp.getFailure().getStatus().getStatus())) {
      return true;
    } else {
      LOG.info("Found non-retryable failure in bulk response: " + itemResp.getFailure().getStatus()
          + ", " + itemResp.getFailure().toString());
      return false;
    }
  }
}
