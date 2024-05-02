/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * OpenSearch Bulk writer. More reading https://opensearch.org/docs/1.2/opensearch/rest-api/document-apis/bulk/.
 * It is not thread safe.
 */
public class OpenSearchWriter extends FlintWriter {

  private final String indexName;

  private final String refreshPolicy;

  private final ByteArrayOutputStream baos;

  private IRestHighLevelClient client;

  public OpenSearchWriter(IRestHighLevelClient client, String indexName, String refreshPolicy,
      int bufferSizeInBytes) {
    this.client = client;
    this.indexName = indexName;
    this.refreshPolicy = refreshPolicy;
    this.baos = new ByteArrayOutputStream(bufferSizeInBytes);
  }

  @Override public void write(char[] cbuf, int off, int len) {
    byte[] bytes = new String(cbuf, off, len).getBytes(StandardCharsets.UTF_8);
    baos.write(bytes, 0, bytes.length);
  }

  /**
   * Flush the data in buffer.
   * Todo. StringWriter is not efficient. it will copy the cbuf when create bytes.
   */
  @Override public void flush() {
    try {
      if (baos.size() > 0) {
        byte[] bytes = baos.toByteArray();
        BulkResponse
            response =
            client.bulk(
                new BulkRequest(indexName).setRefreshPolicy(refreshPolicy).add(bytes, 0, bytes.length, XContentType.JSON),
                RequestOptions.DEFAULT);
        // fail entire bulk request even one doc failed.
        if (response.hasFailures() && Arrays.stream(response.getItems()).anyMatch(itemResp -> !isCreateConflict(itemResp))) {
          throw new RuntimeException(response.buildFailureMessage());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to execute bulk request on index: %s", indexName), e);
    } finally {
      baos.reset();
    }
  }

  @Override public void close() {
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long getBufferSize() {
    return baos.size();
  }

  private boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE && (itemResp.getFailure() == null || itemResp.getFailure()
        .getStatus() == RestStatus.CONFLICT);
  }
}


