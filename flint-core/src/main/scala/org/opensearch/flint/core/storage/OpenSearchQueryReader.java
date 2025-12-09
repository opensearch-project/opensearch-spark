/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.flint.core.metrics.MetricConstants.REQUEST_METADATA_READ_METRIC_PREFIX;

/**
 * {@link OpenSearchReader} using search. https://opensearch.org/docs/latest/api-reference/search/
 */
public class OpenSearchQueryReader extends OpenSearchReader {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchQueryReader.class);

  public OpenSearchQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().indices(indexName).source(searchSourceBuilder));
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    Optional<SearchResponse> response = Optional.empty();
    try {
      response = Optional.of(client.search(request, RequestOptions.DEFAULT));
      IRestHighLevelClient.recordOperationSuccess(REQUEST_METADATA_READ_METRIC_PREFIX);
    } catch (Exception e) {
      IRestHighLevelClient.recordOperationFailure(REQUEST_METADATA_READ_METRIC_PREFIX, e);
    }
    return response;
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}
