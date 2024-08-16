package org.opensearch.flint.core.storage;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.rest.RestStatus;

public class OpenSearchBulkRetryWrapper {

  private static final Logger LOG = Logger.getLogger(OpenSearchBulkRetryWrapper.class.getName());

  private final RetryPolicy<BulkResponse> retryPolicy;

  public OpenSearchBulkRetryWrapper(FlintRetryOptions retryOptions) {
    this.retryPolicy = retryOptions.getBulkRetryPolicy(bulkItemErrorResultPredicate);
  }

  public BulkResponse withRetry(Callable<BulkResponse> operation) {
    try {
      return Failsafe
          .with(retryPolicy)
          .get(operation::call);
    } catch (FailsafeException ex) {
      LOG.severe("Request failed permanently. Re-throwing original exception.");

      // Failsafe will wrap checked exception, such as ExecutionException
      // So here we have to unwrap failsafe exception and rethrow it
      Throwable cause = ex.getCause();
      throw new RuntimeException(cause);
    }
  }

  /** A predicate to decide if a BulkResponse is retryable or not. */
  private static final CheckedPredicate<BulkResponse> bulkItemErrorResultPredicate = new CheckedPredicate<>() {
    public boolean test(BulkResponse bulkResponse) {
      return bulkResponse.hasFailures() && isRetryable(bulkResponse);
    }

    private boolean isRetryable(BulkResponse bulkResponse) {
      if (Arrays.stream(bulkResponse.getItems())
          .anyMatch(itemResp -> !isCreateConflict(itemResp))) {
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
  };
}
