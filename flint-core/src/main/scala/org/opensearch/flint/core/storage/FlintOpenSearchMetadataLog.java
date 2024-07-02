/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import static java.util.logging.Level.SEVERE;
import static org.opensearch.flint.core.storage.FlintMetadataLogEntryOpenSearchConverter.constructLogEntry;
import static org.opensearch.flint.core.storage.FlintMetadataLogEntryOpenSearchConverter.toJson;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
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
import org.opensearch.flint.common.metadata.log.FlintMetadataLog;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;

/**
 * Flint metadata log in OpenSearch store. For now use single doc instead of maintaining history
 * of metadata log.
 */
public class FlintOpenSearchMetadataLog implements FlintMetadataLog<FlintMetadataLogEntry> {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchMetadataLog.class.getName());

  /**
   * Flint options to create Rest OpenSearch client
   */
  private final FlintOptions options;

  /**
   * Reuse query request index as Flint metadata log store
   */
  private final String metadataLogIndexName;
  private final String dataSourceName;

  /**
   * Doc id for latest log entry (Naming rule is static so no need to query Flint index metadata)
   */
  private final String latestId;

  public FlintOpenSearchMetadataLog(FlintOptions options, String flintIndexName, String metadataLogIndexName) {
    this.options = options;
    this.metadataLogIndexName = metadataLogIndexName;
    this.dataSourceName = options.getDataSourceName();
    this.latestId = Base64.getEncoder().encodeToString(flintIndexName.getBytes());
  }

  @Override
  public FlintMetadataLogEntry add(FlintMetadataLogEntry logEntry) {
    // TODO: use single doc for now. this will be always append in future.
    FlintMetadataLogEntry latest;
    if (!exists()) {
      String errorMsg = "Flint Metadata Log index not found " + metadataLogIndexName;
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
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      GetResponse response =
          client.get(new GetRequest(metadataLogIndexName, latestId), RequestOptions.DEFAULT);

      if (response.isExists()) {
        FlintMetadataLogEntry latest = constructLogEntry(
            response.getId(),
            response.getSeqNo(),
            response.getPrimaryTerm(),
            response.getSourceAsMap()
        );

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
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      DeleteResponse response =
          client.delete(
              new DeleteRequest(metadataLogIndexName, latestId), RequestOptions.DEFAULT);

      LOG.info("Purged log entry with result " + response.getResult());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to purge log entry", e);
    }
  }

  @Override
  public FlintMetadataLogEntry emptyLogEntry() {
    return new FlintMetadataLogEntry(
        "",
        0L,
        FlintMetadataLogEntry.IndexState$.MODULE$.EMPTY(),
        Map.of("seqNo", UNASSIGNED_SEQ_NO, "primaryTerm", UNASSIGNED_PRIMARY_TERM),
        "",
        Map.of("dataSourceName", dataSourceName));
  }

  public FlintMetadataLogEntry failLogEntry(String error) {
    return new FlintMetadataLogEntry(
        "",
        0L,
        FlintMetadataLogEntry.IndexState$.MODULE$.FAILED(),
        Map.of("seqNo", UNASSIGNED_SEQ_NO, "primaryTerm", UNASSIGNED_PRIMARY_TERM),
        error,
        Map.of("dataSourceName", dataSourceName));
  }

  private FlintMetadataLogEntry createLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Creating log entry " + logEntry);
    // Assign doc ID here
    FlintMetadataLogEntry logEntryWithId =
        logEntry.copy(
            latestId,
            logEntry.createTime(),
            logEntry.state(),
            logEntry.entryVersion(),
            logEntry.error(),
            logEntry.storageContext());

    return writeLogEntry(logEntryWithId,
        client -> client.index(
            new IndexRequest()
                .index(metadataLogIndexName)
                .id(logEntryWithId.id())
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .source(toJson(logEntryWithId), XContentType.JSON),
            RequestOptions.DEFAULT));
  }

  private FlintMetadataLogEntry updateLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Updating log entry " + logEntry);
    return writeLogEntry(logEntry,
        client -> client.update(
            new UpdateRequest(metadataLogIndexName, logEntry.id())
                .doc(toJson(logEntry), XContentType.JSON)
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .setIfSeqNo((Long) logEntry.entryVersion().get("seqNo").get())
                .setIfPrimaryTerm((Long) logEntry.entryVersion().get("primaryTerm").get()),
            RequestOptions.DEFAULT));
  }

  private FlintMetadataLogEntry writeLogEntry(
      FlintMetadataLogEntry logEntry,
      CheckedFunction<IRestHighLevelClient, DocWriteResponse> write) {
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      // Write (create or update) the doc
      DocWriteResponse response = write.apply(client);

      // Copy latest seqNo and primaryTerm after write
      logEntry = new FlintMetadataLogEntry(
          logEntry.id(),
          logEntry.createTime(),
          logEntry.state(),
          Map.of("seqNo", response.getSeqNo(), "primaryTerm", response.getPrimaryTerm()),
          logEntry.error(),
          logEntry.storageContext());

      LOG.info("Log entry written as " + logEntry);
      return logEntry;
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to write log entry " + logEntry, e);
    }
  }

  private boolean exists() {
    LOG.info("Checking if Flint index exists " + metadataLogIndexName);
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      return client.doesIndexExist(new GetIndexRequest(metadataLogIndexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + metadataLogIndexName, e);
    }
  }

  private IRestHighLevelClient createOpenSearchClient() {
    return OpenSearchClientUtils.createClient(options);
  }

  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws IOException;
  }
}
