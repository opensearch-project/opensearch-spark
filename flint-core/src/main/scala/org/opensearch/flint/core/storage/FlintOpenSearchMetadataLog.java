/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
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
  private final String indexName;

  /**
   * Doc id for latest log entry (Naming rule is static so no need to query Flint index metadata)
   */
  private final String latestId;

  public FlintOpenSearchMetadataLog(FlintClient flintClient, String flintIndexName, String indexName) {
    this.flintClient = flintClient;
    this.indexName = indexName;
    this.latestId = Base64.getEncoder().encodeToString(flintIndexName.getBytes());
  }

  @Override
  public FlintMetadataLogEntry add(FlintMetadataLogEntry logEntry) {
    // TODO: use single doc for now. this will be always append in future.
    FlintMetadataLogEntry latest;
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
    try (RestHighLevelClient client = flintClient.createClient()) {
      GetResponse response =
          client.get(new GetRequest(indexName, latestId), RequestOptions.DEFAULT);

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

  private FlintMetadataLogEntry createLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Creating log entry " + logEntry);
    try (RestHighLevelClient client = flintClient.createClient()) {
      // Assign doc ID here
      logEntry = logEntry.copy(
          latestId,
          logEntry.seqNo(),
          logEntry.primaryTerm(),
          logEntry.state(),
          logEntry.dataSource(),
          logEntry.error());

      IndexResponse response = client.index(
          new IndexRequest()
              .index(indexName)
              .id(logEntry.id())
              .source(logEntry.toJson(), XContentType.JSON),
          RequestOptions.DEFAULT);

      // Update seqNo and primaryTerm in log entry
      logEntry = logEntry.copy(
          logEntry.id(),
          response.getSeqNo(),
          response.getPrimaryTerm(),
          logEntry.state(),
          logEntry.dataSource(),
          logEntry.error());

      LOG.info("Log entry created " + logEntry);
      return logEntry;
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to create initial log entry", e);
    }
  }

  private FlintMetadataLogEntry updateLogEntry(FlintMetadataLogEntry logEntry) {
    LOG.info("Updating log entry " + logEntry);
    try (RestHighLevelClient client = flintClient.createClient()) {
      UpdateResponse response =
          client.update(
              new UpdateRequest(indexName, logEntry.id())
                  .doc(logEntry.toJson(), XContentType.JSON)
                  .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                  .setIfSeqNo(logEntry.seqNo())
                  .setIfPrimaryTerm(logEntry.primaryTerm()),
              RequestOptions.DEFAULT);

      // Update seqNo and primaryTerm in log entry
      logEntry = logEntry.copy(
          logEntry.id(),
          response.getSeqNo(),
          response.getPrimaryTerm(),
          logEntry.state(),
          logEntry.dataSource(),
          logEntry.error());

      LOG.info("Log entry updated " + logEntry);
      return logEntry;
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to update log entry: " + logEntry, e);
    }
  }

  private FlintMetadataLogEntry writeLogEntry(
      FlintMetadataLogEntry logEntry,
      Function<RestHighLevelClient, DocWriteResponse> write) {
    try (RestHighLevelClient client = flintClient.createClient()) {
      DocWriteResponse response = write.apply(client);

      // Update seqNo and primaryTerm in log entry
      logEntry = logEntry.copy(
          logEntry.id(),
          response.getSeqNo(),
          response.getPrimaryTerm(),
          logEntry.state(),
          logEntry.dataSource(),
          logEntry.error());

      LOG.info("Log entry written as " + logEntry);
      return logEntry;
    } catch (OpenSearchException | IOException e) {
      throw new IllegalStateException("Failed to write log entry " + logEntry, e);
    }
  }
}
