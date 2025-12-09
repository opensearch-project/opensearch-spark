/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.MetaData;
import org.opensearch.flint.core.storage.OpenSearchClientUtils;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

public class OpenSearchCluster {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchCluster.class);

  /**
   * Creates list of OpenSearchIndexTable instance of indices in OpenSearch domain.
   *
   * @param indexName
   *   tableName support (1) single index name. (2) wildcard index name. (3) comma sep index name.
   * @param options
   *   The options for Flint.
   * @return
   *   A list of OpenSearchIndexTable instance.
   */
  public static List<OpenSearchIndexTable> apply(String indexName, FlintOptions options) {
    return getAllOpenSearchTableMetadata(options, indexName.split(","))
        .stream()
        .map(metadata -> new OpenSearchIndexTable(metadata, options))
        .collect(Collectors.toList());
  }

  /**
   * Retrieve all metadata for OpenSearch table whose name matches the given pattern.
   *
   * @param options The options for Flint.
   * @param indexNamePattern index name pattern
   * @return list of OpenSearch table metadata
   */
  public static List<MetaData> getAllOpenSearchTableMetadata(FlintOptions options, String... indexNamePattern) {
    LOG.info("Fetching all OpenSearch table metadata for pattern " + String.join(",", indexNamePattern));
    String[] indexNames =
        Arrays.stream(indexNamePattern).map(OpenSearchClientUtils::sanitizeIndexName).toArray(String[]::new);
    try (IRestHighLevelClient client = OpenSearchClientUtils.createClient(options)) {
      GetIndexRequest request = new GetIndexRequest(indexNames);
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);

      return Arrays.stream(response.getIndices())
          .map(index -> new MetaData(
              index,
              response.getMappings().get(index).source().string(),
              response.getSettings().get(index).toString()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to get OpenSearch table metadata for " +
          String.join(",", indexNames), e);
    }
  }
}
