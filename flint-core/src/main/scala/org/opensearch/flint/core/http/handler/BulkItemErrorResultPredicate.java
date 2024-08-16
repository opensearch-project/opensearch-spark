package org.opensearch.flint.core.http.handler;

import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.logging.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.rest.RestStatus;

// A predicate to check if bulk response has retryable item
public class BulkItemErrorResultPredicate<T> implements CheckedPredicate<T> {
  private static final Logger LOG = Logger.getLogger(BulkItemErrorResultPredicate.class.getName());

  @Override
  public boolean test(T result) throws Throwable {
    BulkResponse bulkResponse = (BulkResponse) result;
    return bulkResponse.hasFailures() && isRetryable(bulkResponse);
  }

  private boolean isRetryable(BulkResponse bulkResponse) {
    if (Arrays.stream(bulkResponse.getItems()).anyMatch(itemResp -> !isCreateConflict(itemResp))) {
      LOG.info("Found retryable failure in the bulk response");
      return true;
    }
    return false;
  }

  private boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE && (itemResp.getFailure() == null
        || itemResp.getFailure()
        .getStatus() == RestStatus.CONFLICT);
  }
}
