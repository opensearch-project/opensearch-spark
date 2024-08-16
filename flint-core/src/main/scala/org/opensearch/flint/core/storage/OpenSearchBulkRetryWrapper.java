package org.opensearch.flint.core.storage;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.rest.RestStatus;

public class OpenSearchBulkRetryWrapper {

  private static final Logger LOG = Logger.getLogger(OpenSearchBulkRetryWrapper.class.getName());

  private final RetryPolicy<BulkResponse> retryPolicy;

  public OpenSearchBulkRetryWrapper(FlintRetryOptions retryOptions) {
    this.retryPolicy = retryOptions.getBulkRetryPolicy(bulkItemErrorResultPredicate);
  }

  public BulkResponse bulkWithPartialRetry(RestHighLevelClient client, BulkRequest bulkRequest,
      RequestOptions options) {
    try {
      final Holder<BulkRequest> nextRequest = new Holder<>(bulkRequest);
      return Failsafe
          .with(retryPolicy)
          .get(() -> {
            BulkResponse response = client.bulk(nextRequest.get(), options);
            if (retryPolicy.getConfig().allowsRetries() && bulkItemErrorResultPredicate.test(
                response)) {
              nextRequest.set(getRetryableRequest(nextRequest.get(), response));
            }
            return response;
          });
    } catch (FailsafeException ex) {
      LOG.severe("Request failed permanently. Re-throwing original exception.");

      // unwrap original exception and throw
      Throwable cause = ex.getCause();
      throw new RuntimeException(cause);
    }
  }

  // Holder class to let lambda expression update next BulkRequest
  private static class Holder<T> {

    private T item;

    public Holder(T item) {
      this.item = item;
    }

    public T get() {
      return item;
    }

    public void set(T item) {
      this.item = item;
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

  private static boolean isRetryable(BulkResponse bulkResponse) {
    if (Arrays.stream(bulkResponse.getItems())
        .anyMatch(itemResp -> !isCreateConflict(itemResp))) {
      LOG.info("Found retryable failure in the bulk response");
      return true;
    }
    return false;
  }

  private static boolean isItemRetryable(BulkItemResponse itemResponse) {
    return itemResponse.isFailed() && !isCreateConflict(itemResponse);
  }

  private static boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE && (itemResp.getFailure() == null
        || itemResp.getFailure()
        .getStatus() == RestStatus.CONFLICT);
  }

  /**
   * A predicate to decide if a BulkResponse is retryable or not.
   */
  private static final CheckedPredicate<BulkResponse> bulkItemErrorResultPredicate = bulkResponse ->
      bulkResponse.hasFailures() && isRetryable(bulkResponse);
}
