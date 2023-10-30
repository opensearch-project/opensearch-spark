/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;

/**
 * Optimistic transaction implementation by OpenSearch OCC.
 * For now use single doc instead of maintaining history of metadata log.
 *
 * @param <T> result type
 */
public class OpenSearchOptimisticTransaction<T> implements OptimisticTransaction<T> {

  /**
   * Flint client to create Rest OpenSearch client (This will be refactored later)
   */
  private final FlintClient flintClient;

  /**
   * Reuse query request index as Flint metadata log store
   */
  private final String metadataLogIndexName;

  /**
   * Doc id for latest log entry (Naming rule is static so no need to query Flint index metadata)
   */
  private final String latestId;

  private Predicate<FlintMetadataLogEntry> initialCondition = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> transientAction = null;
  private Function<FlintMetadataLogEntry, FlintMetadataLogEntry> finalAction = null;

  public OpenSearchOptimisticTransaction(FlintClient flintClient, String flintIndexName, String metadataLogIndexName) {
    this.flintClient = flintClient;
    this.latestId = Base64.getEncoder().encodeToString(flintIndexName.getBytes());
    this.metadataLogIndexName = metadataLogIndexName;
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

    try {
      FlintMetadataLogEntry latest = getLatestLogEntry();
      if (initialCondition.test(latest)) {
        // TODO: log entry can be same?
        createOrUpdateDoc(transientAction.apply(latest));

        T result = operation.get();

        // TODO: don't get latest, use previous entry again. (set seqno in update)
        createOrUpdateDoc(finalAction.apply(getLatestLogEntry()));
        return result;
      } else {
        throw new IllegalStateException();
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  private FlintMetadataLogEntry getLatestLogEntry() {
    try (RestHighLevelClient client = flintClient.createClient()) {
      GetResponse response =
          client.get(new GetRequest(metadataLogIndexName, latestId), RequestOptions.DEFAULT);
      return new FlintMetadataLogEntry(
          response.getId(),
          response.getSeqNo(),
          response.getPrimaryTerm(),
          response.getSourceAsMap());
    } catch (Exception e) { // TODO: resource not found exception?
      return new FlintMetadataLogEntry("", -1, -1, "empty", "mys3", "");
    }
  }

  private void createOrUpdateDoc(FlintMetadataLogEntry logEntry) throws IOException {
    try (RestHighLevelClient client = flintClient.createClient()) {
      DocWriteResponse response;
      if (logEntry.id().isEmpty()) { // TODO: Only create before initialLog for the first time
        logEntry.id_$eq(latestId);
        response = client.index(
            new IndexRequest()
                .index(metadataLogIndexName)
                .id(logEntry.id())
                .source(logEntry.toJson(), XContentType.JSON),
            RequestOptions.DEFAULT);
      } else {
        response =
            client.update(
                new UpdateRequest(metadataLogIndexName, logEntry.id())
                    .doc(logEntry.toJson(), XContentType.JSON)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setIfSeqNo(logEntry.seqNo())
                    .setIfPrimaryTerm(logEntry.primaryTerm()),
                RequestOptions.DEFAULT);
      }

      // Update seqNo and primaryTerm in log entry object
      logEntry.seqNo_$eq(response.getSeqNo()); // TODO: convert log entry to Java class?
      logEntry.primaryTerm_$eq(response.getPrimaryTerm());
    }
  }
}
