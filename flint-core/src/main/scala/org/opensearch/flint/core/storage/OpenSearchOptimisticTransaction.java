/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState$;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
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

  private static final Logger LOG = Logger.getLogger(OpenSearchOptimisticTransaction.class.getName());

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
  public T execute(Function<FlintMetadataLogEntry, T> operation) {
    Objects.requireNonNull(initialCondition);
    Objects.requireNonNull(transientAction);
    Objects.requireNonNull(finalAction);

    FlintMetadataLogEntry latest = getLatestLogEntry();
    if (latest.id().isEmpty()) {
      latest = createLogEntry(latest);
    }

    if (initialCondition.test(latest)) {
      // TODO: log entry can be same?
      latest = updateLogEntry(transientAction.apply(latest));

      T result = operation.apply(latest);

      updateLogEntry(finalAction.apply(latest));
      return result;
    } else {
      throw new IllegalStateException("Exit due to initial log precondition not satisfied");
    }
  }

  // TODO: Move all these to FlintLogEntry <- FlintOpenSearchLogEntry

  private FlintMetadataLogEntry getLatestLogEntry() {
    RestHighLevelClient client = flintClient.createClient();
    try {
      GetResponse response =
          client.get(new GetRequest(metadataLogIndexName, latestId), RequestOptions.DEFAULT);

      if (response.isExists()) {
        return new FlintMetadataLogEntry(
            response.getId(),
            response.getSeqNo(),
            response.getPrimaryTerm(),
            response.getSourceAsMap());
      } else {
        return new FlintMetadataLogEntry("", -1, -1, IndexState$.MODULE$.EMPTY(), "mys3", "");
      }
    } catch (Exception e) { // TODO: resource not found exception?
      throw new IllegalStateException("Failed to fetch latest metadata log entry", e);
    }
  }

  private FlintMetadataLogEntry createLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Creating log entry " + logEntry);
    try (RestHighLevelClient client = flintClient.createClient()) {
      logEntry = logEntry.copy(
          latestId,
          logEntry.seqNo(), logEntry.primaryTerm(), logEntry.state(), logEntry.dataSource(), logEntry.error());

      IndexResponse response = client.index(
          new IndexRequest()
              .index(metadataLogIndexName)
              .id(logEntry.id())
              .source(logEntry.toJson(), XContentType.JSON),
          RequestOptions.DEFAULT);

      // Update seqNo and primaryTerm in log entry object
      return logEntry.copy(
          logEntry.id(), response.getSeqNo(), response.getPrimaryTerm(),
          logEntry.state(), logEntry.dataSource(), logEntry.error());
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to create initial log entry", e);
    }
  }

  private FlintMetadataLogEntry updateLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Updating log entry " + logEntry);
    try (RestHighLevelClient client = flintClient.createClient()) {
      UpdateResponse response =
          client.update(
              new UpdateRequest(metadataLogIndexName, logEntry.id())
                  .doc(logEntry.toJson(), XContentType.JSON)
                  .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                  .setIfSeqNo(logEntry.seqNo())
                  .setIfPrimaryTerm(logEntry.primaryTerm()),
              RequestOptions.DEFAULT);

      // Update seqNo and primaryTerm in log entry object
      return logEntry.copy(
          logEntry.id(), response.getSeqNo(), response.getPrimaryTerm(),
          logEntry.state(), logEntry.dataSource(), logEntry.error());
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to update log entry: " + logEntry, e);
    }
  }
}
