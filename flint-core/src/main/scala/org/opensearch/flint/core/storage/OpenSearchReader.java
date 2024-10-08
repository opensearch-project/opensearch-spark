/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Abstract OpenSearch Reader.
 */
public abstract class OpenSearchReader implements FlintReader {
  @VisibleForTesting
  /** Search request source builder. */
  public final SearchRequest searchRequest;

  protected final IRestHighLevelClient client;

  /**
   * iterator of one-shot search result.
   */
  private Iterator<SearchHit> iterator = null;

  public OpenSearchReader(IRestHighLevelClient client, SearchRequest searchRequest) {
    this.client = client;
    this.searchRequest = searchRequest;
  }

  @Override public boolean hasNext() {
    try {
      if (iterator == null || !iterator.hasNext()) {
        Optional<SearchResponse> response = search(searchRequest);
        if (response.isEmpty()) {
          iterator = null;
          return false;
        }
        List<SearchHit> searchHits = Arrays.asList(response.get().getHits().getHits());
        iterator = searchHits.iterator();
      }
      return iterator.hasNext();
    } catch (OpenSearchStatusException e) {
      //  e.g., org.opensearch.OpenSearchStatusException: OpenSearch exception [type=index_not_found_exception, reason=no such index [query_results2]]
      if (e.getMessage() != null && (e.getMessage().contains("index_not_found_exception"))) {
        return false;
      } else {
        throw e;
      }
    } catch (IOException e) {
      // todo. log error.
      throw new RuntimeException(e);
    }
  }

  @Override public String next() {
    return iterator.next().getSourceAsString();
  }

  @Override public void close() {
    try {
      clean();
    } catch (IOException e) {
      // todo. log error.
    } finally {
      if (client != null) {
        try {
          client.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * search.
   */
  abstract Optional<SearchResponse> search(SearchRequest request) throws IOException;

  /**
   * clean.
   */
  abstract void clean() throws IOException;
}
