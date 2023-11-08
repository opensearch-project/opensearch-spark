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
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link OpenSearchReader} using scroll search. https://opensearch.org/docs/latest/api-reference/scroll/
 */
public class OpenSearchScrollReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchScrollReader.class.getName());

  /** Default scroll context timeout in minutes. */
  public static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(5L);

  private final FlintOptions options;

  private String scrollId = null;

  public OpenSearchScrollReader(RestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder, FlintOptions options) {
    super(client, new SearchRequest().indices(indexName).source(searchSourceBuilder.size(options.getScrollSize())));
    this.options = options;
  }

  /**
   * search.
   */
  SearchResponse search(SearchRequest request) throws IOException {
    if (Strings.isNullOrEmpty(scrollId)) {
      // add scroll timeout making the request as scroll search request.
      request.scroll(DEFAULT_SCROLL_TIMEOUT);
      SearchResponse response = client.search(request, RequestOptions.DEFAULT);
      scrollId = response.getScrollId();
      return response;
    } else {
      return client.scroll(new SearchScrollRequest().scroll(DEFAULT_SCROLL_TIMEOUT).scrollId(scrollId), RequestOptions.DEFAULT);
    }
  }

  /**
   * clean the scroll context.
   */
  void clean() throws IOException {
    try {
      if (!Strings.isNullOrEmpty(scrollId)) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
      }
    } catch (OpenSearchStatusException e) {
      // OpenSearch throw exception if scroll already closed. https://github.com/opensearch-project/OpenSearch/issues/11121
      LOG.log(Level.WARNING, "close scroll exception, it is a known bug https://github" +
          ".com/opensearch-project/OpenSearch/issues/11121.", e);
    }
  }
}
