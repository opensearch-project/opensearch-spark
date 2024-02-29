/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;

import java.io.IOException;

import static org.opensearch.flint.core.metrics.MetricConstants.*;

/**
 * A wrapper class for RestHighLevelClient to facilitate OpenSearch operations
 * with integrated metrics tracking.
 */
public class RestHighLevelClientWrapper implements IRestHighLevelClient {
    private final RestHighLevelClient client;

    /**
     * Constructs a new RestHighLevelClientWrapper.
     *
     * @param client the RestHighLevelClient instance to wrap
     */
    public RestHighLevelClientWrapper(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.bulk(bulkRequest, options));
    }

    @Override
    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.clearScroll(clearScrollRequest, options));
    }

    @Override
    public CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest, RequestOptions options) throws IOException {
        return execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.indices().create(createIndexRequest, options));
    }

    @Override
    public void deleteIndex(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException {
        execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.indices().delete(deleteIndexRequest, options));
    }

    @Override
    public DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException {
        return execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.delete(deleteRequest, options));
    }

    @Override
    public GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.get(getRequest, options));
    }

    @Override
    public GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.indices().get(getIndexRequest, options));
    }

    @Override
    public IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException {
        return execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.index(indexRequest, options));
    }

    @Override
    public Boolean doesIndexExist(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.indices().exists(getIndexRequest, options));
    }

    @Override
    public SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.search(searchRequest, options));
    }

    @Override
    public SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return execute(OS_READ_OP_METRIC_PREFIX, () -> client.scroll(searchScrollRequest, options));
    }

    @Override
    public UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException {
        return execute(OS_WRITE_OP_METRIC_PREFIX, () -> client.update(updateRequest, options));
    }

    /**
     * Executes a given operation, tracks metrics, and handles exceptions.
     *
     * @param metricNamePrefix the prefix for the metric name
     * @param operation        the operation to execute
     * @param <T>              the return type of the operation
     * @return the result of the operation
     * @throws IOException if an I/O exception occurs
     */
    private <T> T execute(String metricNamePrefix, IOCallable<T> operation) throws IOException {
        try {
            T result = operation.call();
            IRestHighLevelClient.recordOperationSuccess(metricNamePrefix);
            return result;
        } catch (Exception e) {
            IRestHighLevelClient.recordOperationFailure(metricNamePrefix, e);
            throw e;
        }
    }

    /**
     * Functional interface for operations that can throw IOException.
     *
     * @param <T> the return type of the operation
     */
    @FunctionalInterface
    private interface IOCallable<T> {
        T call() throws IOException;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
