/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.common.metadata.FlintMetadata;
import org.opensearch.flint.core.FlintClient;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;
import scala.Option;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Flint client implementation for OpenSearch storage.
 */
public class FlintOpenSearchClient implements FlintClient {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchClient.class.getName());

  private final FlintOptions options;

  public FlintOpenSearchClient(FlintOptions options) {
    this.options = options;
  }

  @Override
  public void createIndex(String indexName, FlintMetadata metadata) {
    LOG.info("Creating Flint index " + indexName + " with metadata " + metadata);
    try {
      createIndex(indexName, FlintOpenSearchIndexMetadataService.serialize(metadata, false), metadata.indexSettings());
      emitIndexCreationSuccessMetric(metadata.kind());
    } catch (IllegalStateException ex) {
      emitIndexCreationFailureMetric(metadata.kind());
      throw ex;
    }
  }

  protected void createIndex(String indexName, String mapping, Option<String> settings) {
    LOG.info("Creating Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      CreateIndexRequest request = new CreateIndexRequest(osIndexName);
      request.mapping(mapping, XContentType.JSON);
      if (settings.isDefined()) {
        request.settings(settings.get(), XContentType.JSON);
      }
      client.createIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Flint index " + osIndexName, e);
    }
  }

  @Override
  public boolean exists(String indexName) {
    LOG.info("Checking if Flint index exists " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      return client.doesIndexExist(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if Flint index exists " + osIndexName, e);
    }
  }

  @Override
  public List<String> getIndexNames(String... indexNamePatterns) {
    LOG.info("Getting Flint index names for pattern " + String.join(",", indexNamePatterns));
    String[] osIndexNamePatterns = Arrays.stream(indexNamePatterns).map(this::sanitizeIndexName).toArray(String[]::new);
    try (IRestHighLevelClient client = createClient()) {
      GetIndexRequest request = new GetIndexRequest(osIndexNamePatterns);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);
      return Arrays.stream(response.getIndices()).collect(Collectors.toList());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get Flint index names for pattern " + String.join(", ", indexNamePatterns), e);
    }
  }

  @Override
  public void deleteIndex(String indexName) {
    LOG.info("Deleting Flint index " + indexName);
    String osIndexName = sanitizeIndexName(indexName);
    try (IRestHighLevelClient client = createClient()) {
      DeleteIndexRequest request = disableTimeoutsForServerless(
          new DeleteIndexRequest(osIndexName)
      );
      client.deleteIndex(request, RequestOptions.DEFAULT);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete Flint index " + osIndexName, e);
    }
  }

  /** OpenSearch Serverless does not accept timeout parameters for deleteIndex API */
  private DeleteIndexRequest disableTimeoutsForServerless(DeleteIndexRequest deleteIndexRequest) {
    if (FlintOptions.SERVICE_NAME_AOSS.equals(options.getServiceName())) {
      return deleteIndexRequest
          .clusterManagerNodeTimeout((TimeValue) null)
          .timeout((TimeValue) null);
    } else {
      return deleteIndexRequest;
    }
  }

  public FlintWriter createWriter(String indexName) {
    LOG.info(String.format("Creating Flint index writer for %s, refresh_policy:%s, " +
        "batch_bytes:%d", indexName, options.getRefreshPolicy(), options.getBatchBytes()));
    return new OpenSearchWriter(createClient(), sanitizeIndexName(indexName),
        options.getRefreshPolicy(), options.getBatchBytes());
  }

  @Override
  public IRestHighLevelClient createClient() {
    return OpenSearchClientUtils.createClient(options);
  }

  private String sanitizeIndexName(String indexName) {
    return OpenSearchClientUtils.sanitizeIndexName(indexName);
  }

  private void emitIndexCreationSuccessMetric(String indexKind) {
    emitIndexCreationMetric(indexKind, "success");
  }

  private void emitIndexCreationFailureMetric(String indexKind) {
    emitIndexCreationMetric(indexKind, "failed");
  }

  private void emitIndexCreationMetric(String indexKind, String status) {
    switch (indexKind) {
      case "skipping":
        MetricsUtil.addHistoricGauge(String.format("%s.%s.count", MetricConstants.CREATE_SKIPPING_INDICES, status), 1);
        break;
      case "covering":
        MetricsUtil.addHistoricGauge(String.format("%s.%s.count", MetricConstants.CREATE_COVERING_INDICES, status), 1);
        break;
      case "mv":
        MetricsUtil.addHistoricGauge(String.format("%s.%s.count", MetricConstants.CREATE_MV_INDICES, status), 1);
        break;
      default:
        break;
    }
  }
}
