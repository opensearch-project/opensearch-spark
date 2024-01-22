/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link OpenSearchReader} using search. https://opensearch.org/docs/latest/api-reference/search/
 */
public class OpenSearchQueryReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchQueryReader.class.getName());

  public OpenSearchQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().indices(indexName).source(searchSourceBuilder));
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) throws IOException {
   return Optional.of(client.search(request, RequestOptions.DEFAULT));
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}
