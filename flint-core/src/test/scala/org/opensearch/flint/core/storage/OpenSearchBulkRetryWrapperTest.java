package org.opensearch.flint.core.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.rest.RestStatus;

@ExtendWith(MockitoExtension.class)
class OpenSearchBulkRetryWrapperTest {

  @Mock
  BulkResponse bulkResponse;
  @Mock
  Callable<BulkResponse> callable;
  @Mock
  DocWriteResponse docWriteResponse;
  BulkItemResponse successItem = new BulkItemResponse(0, OpType.CREATE, docWriteResponse);
  BulkItemResponse failureItem = new BulkItemResponse(0, OpType.CREATE,
      new Failure("index", "id", null,
          RestStatus.TOO_MANY_REQUESTS));

  FlintRetryOptions retryOptionsWithRetry = new FlintRetryOptions(Map.of("retry.max_retries", "2"));
  FlintRetryOptions retryOptionsWithoutRetry = new FlintRetryOptions(
      Map.of("retry.max_retries", "0"));

  @Test
  public void withRetryWhenCallSucceed() throws Exception {
    OpenSearchBulkRetryWrapper bulkRetryWrapper = new OpenSearchBulkRetryWrapper(
        retryOptionsWithRetry);
    when(callable.call()).thenReturn(bulkResponse);
    when(bulkResponse.hasFailures()).thenReturn(false);

    BulkResponse response = bulkRetryWrapper.withRetry(callable);

    assertEquals(response, bulkResponse);
    verify(callable).call();
  }

  @Test
  public void withRetryWhenCallFailOnce() throws Exception {
    OpenSearchBulkRetryWrapper bulkRetryWrapper = new OpenSearchBulkRetryWrapper(
        retryOptionsWithRetry);
    when(callable.call())
        .thenReturn(bulkResponse)
        .thenReturn(bulkResponse);
    when(bulkResponse.hasFailures())
        .thenReturn(true)
        .thenReturn(false);
    when(bulkResponse.getItems())
        .thenReturn(new BulkItemResponse[]{successItem, failureItem})
        .thenReturn(new BulkItemResponse[]{successItem, successItem});

    BulkResponse response = bulkRetryWrapper.withRetry(callable);

    assertEquals(response, bulkResponse);
    verify(callable, times(2)).call();
  }

  @Test
  public void withRetryWhenCallThrowsShouldNotRetry() throws Exception {
    OpenSearchBulkRetryWrapper bulkRetryWrapper = new OpenSearchBulkRetryWrapper(
        retryOptionsWithRetry);
    when(callable.call()).thenThrow(new RuntimeException("test"));

    assertThrows(RuntimeException.class, () -> bulkRetryWrapper.withRetry(callable));

    verify(callable, times(1)).call();
  }

  @Test
  public void withoutRetryWhenCallFail() throws Exception {
    OpenSearchBulkRetryWrapper bulkRetryWrapper = new OpenSearchBulkRetryWrapper(
        retryOptionsWithoutRetry);
    when(callable.call()).thenReturn(bulkResponse);
    when(bulkResponse.hasFailures()).thenReturn(true);
    when(bulkResponse.getItems())
        .thenReturn(new BulkItemResponse[]{successItem, failureItem});

    BulkResponse response = bulkRetryWrapper.withRetry(callable);

    assertEquals(response, bulkResponse);
    verify(callable).call();
  }
}
