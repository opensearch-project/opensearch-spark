/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for wrapping the OpenSearch High Level REST Client with additional functionality,
 * such as metrics tracking.
 */
public interface IRestHighLevelClient extends Closeable {

    BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException;

    ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException;

    CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest, RequestOptions options) throws IOException;

    void deleteIndex(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException;

    DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException;

    GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException;

    GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException;

    IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException;

    Boolean isIndexExists(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException;

    SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException;

    SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException;

    DocWriteResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException;
}
