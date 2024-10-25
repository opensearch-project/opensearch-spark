package org.opensearch.flint.core.storage;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.List;
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
import org.opensearch.rest.RestStatus;

public class OpenSearchBulkRetryWrapper {

  private static final Logger LOG = Logger.getLogger(OpenSearchBulkRetryWrapper.class.getName());

  private final RetryPolicy<BulkResponse> retryPolicy;

  public OpenSearchBulkRetryWrapper(FlintRetryOptions retryOptions) {
    this.retryPolicy = retryOptions.getBulkRetryPolicy(bulkItemRetryableResultPredicate);
  }

  /**
   * Delegate bulk request to the client, and retry the request if the response contains retryable
   * failure. It won't retry when bulk call thrown exception.
   * @param client used to call bulk API
   * @param bulkRequest requests passed to bulk method
   * @param options options passed to bulk method
   * @return Last result
   */
  public BulkResponse bulkWithPartialRetry(RestHighLevelClient client, BulkRequest bulkRequest,
      RequestOptions options) {
    final AtomicInteger retryCount = new AtomicInteger(0);
    try {
      final AtomicReference<BulkRequest> nextRequest = new AtomicReference<>(bulkRequest);
      BulkResponse res = Failsafe
          .with(retryPolicy)
          .get(() -> {
            BulkResponse response = client.bulk(nextRequest.get(), options);
            if (retryPolicy.getConfig().allowsRetries() && bulkItemRetryableResultPredicate.test(
                response)) {
              nextRequest.set(getRetryableRequest(nextRequest.get(), response));
              retryCount.incrementAndGet();
            }
            return response;
          });
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, bulkRequest.estimatedSizeInBytes());
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, retryCount.get());
      return res;
    } catch (FailsafeException ex) {
      LOG.severe("Request failed permanently. Re-throwing original exception.");
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, retryCount.get() - 1);
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC, 1);

      // unwrap original exception and throw
      throw new RuntimeException(ex.getCause());
    }
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
  private static final CheckedPredicate<BulkResponse> bulkItemRetryableResultPredicate = bulkResponse ->
      bulkResponse.hasFailures() && isRetryable(bulkResponse);

  private static boolean isRetryable(BulkResponse bulkResponse) {
    if (Arrays.stream(bulkResponse.getItems())
        .anyMatch(itemResp -> isItemRetryable(itemResp))) {
      LOG.info("Found retryable failure in the bulk response");
      return true;
    }
    return false;
  }

  private static boolean isItemRetryable(BulkItemResponse itemResponse) {
    return itemResponse.isFailed() && !isCreateConflict(itemResponse);
  }

  private static boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE &&
        itemResp.getFailure().getStatus() == RestStatus.CONFLICT;
  }
}
