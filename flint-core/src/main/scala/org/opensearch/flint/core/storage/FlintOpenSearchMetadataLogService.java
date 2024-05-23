/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.Logger;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.common.metadata.log.FlintMetadataLog;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState$;
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService;
import org.opensearch.flint.common.metadata.log.OptimisticTransaction;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.log.DefaultOptimisticTransaction;

/**
 * Flint metadata log service implementation for OpenSearch storage.
 */
public class FlintOpenSearchMetadataLogService implements FlintMetadataLogService {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchMetadataLogService.class.getName());

  public final static String METADATA_LOG_INDEX_NAME_PREFIX = ".query_execution_request";

  private final FlintOptions options;
  private final String dataSourceName;
  private final String metadataLogIndexName;

  public FlintOpenSearchMetadataLogService(FlintOptions options) {
    this.options = options;
    this.dataSourceName = options.getDataSourceName();
    this.metadataLogIndexName = constructMetadataLogIndexName();
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName, boolean forceInit) {
    LOG.info("Starting transaction on index " + indexName + " and data source " + dataSourceName);
    Optional<FlintMetadataLog<FlintMetadataLogEntry>> metadataLog = getIndexMetadataLog(indexName, forceInit);
    if (metadataLog.isEmpty()) {
      String errorMsg = "Metadata log index not found " + metadataLogIndexName;
      throw new IllegalStateException(errorMsg);
    }
    return new DefaultOptimisticTransaction<>(metadataLog.get());
  }

  @Override
  public Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName) {
    return getIndexMetadataLog(indexName, false);
  }

  @Override
  public void recordHeartbeat(String indexName) {
    startTransaction(indexName)
        .initialLog(latest -> latest.state() == IndexState$.MODULE$.REFRESHING())
        .finalLog(latest -> latest) // timestamp will update automatically
        .commit(latest -> null);
  }

  private Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName, boolean initIfNotExist) {
    LOG.info("Getting metadata log for index " + indexName + " and data source " + dataSourceName);
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metadataLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metadataLogIndexName);
      } else {
        if (initIfNotExist) {
          initIndexMetadataLog();
        } else {
          String errorMsg = "Metadata log index not found " + metadataLogIndexName;
          LOG.warning(errorMsg);
          return Optional.empty();
        }
      }
      return Optional.of(new FlintOpenSearchMetadataLog(options, indexName, metadataLogIndexName));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metadataLogIndexName, e);
    }
  }

  private void initIndexMetadataLog() {
    LOG.info("Initializing metadata log index " + metadataLogIndexName);
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      CreateIndexRequest request = new CreateIndexRequest(metadataLogIndexName);
      request.mapping(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_MAPPING(), XContentType.JSON);
      request.settings(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_SETTINGS(), XContentType.JSON);
      client.createIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to initialize metadata log index " + metadataLogIndexName, e);
    }
  }

  private String constructMetadataLogIndexName() {
    return dataSourceName.isEmpty() ? METADATA_LOG_INDEX_NAME_PREFIX : METADATA_LOG_INDEX_NAME_PREFIX + "_" + dataSourceName;
  }

  private IRestHighLevelClient createOpenSearchClient() {
    return OpenSearchClientUtils.createClient(options);
  }
}
