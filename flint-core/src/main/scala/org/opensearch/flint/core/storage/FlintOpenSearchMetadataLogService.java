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
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.log.DefaultOptimisticTransaction;
import org.opensearch.flint.core.metadata.log.FlintMetadataLog;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogService;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;

/**
 * Flint metadata log service implementation for OpenSearch storage.
 */
public class FlintOpenSearchMetadataLogService implements FlintMetadataLogService {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchMetadataLogService.class.getName());

  public final static String METADATA_LOG_INDEX_NAME_PREFIX = ".query_execution_request";

  private final FlintOptions options;
  private final String dataSourceName;
  private final String metaLogIndexName;

  public FlintOpenSearchMetadataLogService(FlintOptions options) {
    this.options = options;
    this.dataSourceName = options.getDataSourceName();
    this.metaLogIndexName = constructMetaLogIndexName();
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName, boolean forceInit) {
    LOG.info("Starting transaction on index " + indexName + " and data source " + dataSourceName);
    Optional<FlintMetadataLog<FlintMetadataLogEntry>> metadataLog = getIndexMetadataLog(indexName, forceInit);
    if (metadataLog.isEmpty()) {
      String errorMsg = "Metadata log index not found " + metaLogIndexName;
      throw new IllegalStateException(errorMsg);
    }
    return new DefaultOptimisticTransaction<>(dataSourceName, metadataLog.get());
  }

  @Override
  public Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName) {
    return getIndexMetadataLog(indexName, false);
  }

  private Optional<FlintMetadataLog<FlintMetadataLogEntry>> getIndexMetadataLog(String indexName, boolean initIfNotExist) {
    LOG.info("Getting metadata log for index " + indexName + " and data source " + dataSourceName);
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metaLogIndexName);
      } else {
        if (initIfNotExist) {
          initIndexMetadataLog();
        } else {
          String errorMsg = "Metadata log index not found " + metaLogIndexName;
          LOG.warning(errorMsg);
          return Optional.empty();
        }
      }
      return Optional.of(new FlintOpenSearchMetadataLog(options, indexName, metaLogIndexName));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metaLogIndexName, e);
    }
  }

  private void initIndexMetadataLog() {
    LOG.info("Initializing metadata log index " + metaLogIndexName);
    try (IRestHighLevelClient client = createOpenSearchClient()) {
      CreateIndexRequest request = new CreateIndexRequest(metaLogIndexName);
      request.mapping(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_MAPPING(), XContentType.JSON);
      request.settings(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_SETTINGS(), XContentType.JSON);
      client.createIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to initialize metadata log index " + metaLogIndexName, e);
    }
  }

  private String constructMetaLogIndexName() {
    return dataSourceName.isEmpty() ? METADATA_LOG_INDEX_NAME_PREFIX : METADATA_LOG_INDEX_NAME_PREFIX + "_" + dataSourceName;
  }

  private IRestHighLevelClient createOpenSearchClient() {
    return OpenSearchClientUtils.createClient(options);
  }
}
