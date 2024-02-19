/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static java.util.logging.Level.SEVERE;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.logging.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.log.FlintMetadataLog;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;

/**
 * Flint metadata log in OpenSearch store. For now use single doc instead of maintaining history
 * of metadata log.
 */
public class FlintOpenSearchMetadataLog implements FlintMetadataLog<FlintMetadataLogEntry> {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchMetadataLog.class.getName());

  /**
   * Flint client to create Rest OpenSearch client (This will be refactored later)
   */
  private final FlintClient flintClient;

  /**
   * Reuse query request index as Flint metadata log store
   */
  private final String metaLogIndexName;

  /**
   * Doc id for latest log entry (Naming rule is static so no need to query Flint index metadata)
   */
  private final String latestId;

  public FlintOpenSearchMetadataLog(FlintClient flintClient, String flintIndexName, String metaLogIndexName) {
    this.flintClient = flintClient;
    this.metaLogIndexName = metaLogIndexName;
    this.latestId = Base64.getEncoder().encodeToString(flintIndexName.getBytes());
  }

  @Override
  public FlintMetadataLogEntry add(FlintMetadataLogEntry logEntry) {
    // TODO: use single doc for now. this will be always append in future.
    FlintMetadataLogEntry latest;
    if (!exists()) {
      String errorMsg = "Flint Metadata Log index not found " + metaLogIndexName;
      LOG.log(SEVERE, errorMsg);
      throw new IllegalStateException(errorMsg);
    }
    if (logEntry.id().isEmpty()) {
      latest = createLogEntry(logEntry);
    } else {
      latest = updateLogEntry(logEntry);
    }
    return latest;
  }

  @Override
  public Optional<FlintMetadataLogEntry> getLatest() {
    LOG.info("Fetching latest log entry with id " + latestId);
    try (IRestHighLevelClient client = flintClient.createClient()) {
      GetResponse response =
          client.get(new GetRequest(metaLogIndexName, latestId), RequestOptions.DEFAULT);

      if (response.isExists()) {
        FlintMetadataLogEntry latest = new FlintMetadataLogEntry(
            response.getId(),
            response.getSeqNo(),
            response.getPrimaryTerm(),
            response.getSourceAsMap());

        LOG.info("Found latest log entry " + latest);
        return Optional.of(latest);
      } else {
        LOG.info("Latest log entry not found");
        return Optional.empty();
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to fetch latest metadata log entry", e);
    }
  }

  @Override
  public void purge() {
    LOG.info("Purging log entry with id " + latestId);
    try (IRestHighLevelClient client = flintClient.createClient()) {
      DeleteResponse response =
          client.delete(
              new DeleteRequest(metaLogIndexName, latestId), RequestOptions.DEFAULT);

      LOG.info("Purged log entry with result " + response.getResult());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to purge log entry", e);
    }
  }

  private FlintMetadataLogEntry createLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Creating log entry " + logEntry);
    // Assign doc ID here
    FlintMetadataLogEntry logEntryWithId =
        logEntry.copy(
            latestId,
            logEntry.seqNo(),
            logEntry.primaryTerm(),
            logEntry.createTime(),
            logEntry.state(),
            logEntry.dataSource(),
            logEntry.error());

    return writeLogEntry(logEntryWithId,
        client -> client.index(
            new IndexRequest()
                .index(metaLogIndexName)
                .id(logEntryWithId.id())
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .source(logEntryWithId.toJson(), XContentType.JSON),
            RequestOptions.DEFAULT));
  }

  private FlintMetadataLogEntry updateLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Updating log entry " + logEntry);
    return writeLogEntry(logEntry,
        client -> client.update(
            new UpdateRequest(metaLogIndexName, logEntry.id())
                .doc(logEntry.toJson(), XContentType.JSON)
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .setIfSeqNo(logEntry.seqNo())
                .setIfPrimaryTerm(logEntry.primaryTerm()),
            RequestOptions.DEFAULT));
  }

  private FlintMetadataLogEntry writeLogEntry(
      FlintMetadataLogEntry logEntry,
      CheckedFunction<IRestHighLevelClient, DocWriteResponse> write) {
    try (IRestHighLevelClient client = flintClient.createClient()) {
      // Write (create or update) the doc
      DocWriteResponse response = write.apply(client);

      // Copy latest seqNo and primaryTerm after write
      logEntry = logEntry.copy(
          logEntry.id(),
          response.getSeqNo(),
          response.getPrimaryTerm(),
          logEntry.createTime(),
          logEntry.state(),
          logEntry.dataSource(),
          logEntry.error());

      LOG.info("Log entry written as " + logEntry);
      return logEntry;
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to write log entry " + logEntry, e);
    }
  }

  private boolean exists() {
    LOG.info("Checking if Flint index exists " + metaLogIndexName);
    try (IRestHighLevelClient client = flintClient.createClient()) {
      return client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + metaLogIndexName, e);
    }
  }

  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws IOException;
  }
}
