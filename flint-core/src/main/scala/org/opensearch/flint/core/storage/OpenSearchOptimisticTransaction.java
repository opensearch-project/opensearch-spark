/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;

public class OpenSearchOptimisticTransaction<T> implements OptimisticTransaction<T> {

  private final FlintClient flintClient;

  // Reuse query request index as Flint metadata log store
  private final String metadataLogIndexName = ".query_request_history_mys3"; // TODO: get suffix ds name from Spark conf

  // No need to query Flint index metadata
  private final String latestId;

  private Predicate<FlintMetadataLogEntry> initialCondition = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> transientAction = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> finalAction = null;

  public OpenSearchOptimisticTransaction(String flintIndexName, FlintClient flintClient) {
    this.latestId = Base64.getEncoder().encodeToString(flintIndexName.getBytes());
    this.flintClient = flintClient;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> initialLog(Predicate<FlintMetadataLogEntry> initialCondition) {
    this.initialCondition = initialCondition;
    return this;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> transientLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.transientAction = action;
    return this;
  }

  @Override
  public OpenSearchOptimisticTransaction<T> finalLog(Function<FlintMetadataLogEntry, FlintMetadataLogEntry> action) {
    this.finalAction = action;
    return this;
  }

  @Override
  public T execute(Supplier<T> operation) {
    Objects.requireNonNull(initialCondition);
    Objects.requireNonNull(transientAction);
    Objects.requireNonNull(finalAction);

    FlintMetadataLogEntry latest = getLatestLogEntry();
    if (initialCondition.test(latest)) {
      updateDoc(transientAction.apply(latest));
      T result = operation.get();
      updateDoc(finalAction.apply(getLatestLogEntry()));
      return result;
    } else {
      throw new IllegalStateException();
    }
  }

  private FlintMetadataLogEntry getLatestLogEntry() {
    return getDoc(latestId).orElse(
        new FlintMetadataLogEntry("", -1, -1, "empty", "mys3", ""));
  }

  // Visible for IT
  public Optional<FlintMetadataLogEntry> getDoc(String docId) {
    RestHighLevelClient client = flintClient.createClient();
    try {
      GetResponse response = client.get(new GetRequest(metadataLogIndexName, docId), RequestOptions.DEFAULT);
      return Optional.of(new FlintMetadataLogEntry(response.getId(), response.getSeqNo(), response.getPrimaryTerm(), response.getSourceAsMap()));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private void createDoc(FlintMetadataLogEntry logEntry) {
    try (RestHighLevelClient client = flintClient.createClient()) {
      IndexRequest request = new IndexRequest()
          .index(metadataLogIndexName)
          .id(logEntry.docId())
          .source(logEntry.toJson(), XContentType.JSON);
      client.index(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateDoc(FlintMetadataLogEntry logEntry) {
    if (logEntry.docId().isEmpty()) {
      createDoc(
          logEntry.copy(
              latestId,
              logEntry.seqNo(),
              logEntry.primaryTerm(),
              logEntry.state(),
              logEntry.dataSource(),
              logEntry.error()));
    } else {
      updateIf(logEntry.docId(), logEntry.toJson(), logEntry.seqNo(), logEntry.primaryTerm());
    }
  }

  public void updateIf(String id, String doc, long seqNo, long primaryTerm) {
    try (RestHighLevelClient client = flintClient.createClient()) {
      UpdateRequest updateRequest = new UpdateRequest(metadataLogIndexName, id)
          .doc(doc, XContentType.JSON)
          .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
          .setIfSeqNo(seqNo)
          .setIfPrimaryTerm(primaryTerm);
      UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to execute update request on index: %s, id: %s", metadataLogIndexName, id),
          e);
    }
  }
}
