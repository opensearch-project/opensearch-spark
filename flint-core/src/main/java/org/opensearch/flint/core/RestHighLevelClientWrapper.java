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
import org.opensearch.OpenSearchException;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.flint.core.metrics.MetricsUtil;

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
    public Boolean isIndexExists(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
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
            recordOperationSuccess(metricNamePrefix);
            return result;
        } catch (Exception e) {
            recordOperationFailure(metricNamePrefix, e);
            throw e;
        }
    }

    /**
     * Records the success of an OpenSearch operation by incrementing the corresponding metric counter.
     * This method constructs the metric name by appending ".200.count" to the provided metric name prefix.
     * The metric name is then used to increment the counter, indicating a successful operation.
     *
     * @param metricNamePrefix the prefix for the metric name which is used to construct the full metric name for success
     */
    private void recordOperationSuccess(String metricNamePrefix) {
        String successMetricName = metricNamePrefix + ".2xx.count";
        MetricsUtil.incrementCounter(successMetricName);
    }

    /**
     * Records the failure of an OpenSearch operation by incrementing the corresponding metric counter.
     * If the exception is an OpenSearchException with a specific status code (e.g., 403),
     * it increments a metric specifically for that status code.
     * Otherwise, it increments a general failure metric counter based on the status code category (e.g., 4xx, 5xx).
     *
     * @param metricNamePrefix the prefix for the metric name which is used to construct the full metric name for failure
     * @param e                the exception encountered during the operation, used to determine the type of failure
     */
    private void recordOperationFailure(String metricNamePrefix, Exception e) {
        OpenSearchException openSearchException = extractOpenSearchException(e);
        int statusCode = openSearchException != null ? openSearchException.status().getStatus() : 500;

        if (statusCode == 403) {
            String forbiddenErrorMetricName = metricNamePrefix + ".403.count";
            MetricsUtil.incrementCounter(forbiddenErrorMetricName);
        }

        String failureMetricName = metricNamePrefix + "." + (statusCode / 100) + "xx.count";
        MetricsUtil.incrementCounter(failureMetricName);
    }

    /**
     * Extracts an OpenSearchException from the given Throwable.
     * Checks if the Throwable is an instance of OpenSearchException or caused by one.
     *
     * @param ex the exception to be checked
     * @return the extracted OpenSearchException, or null if not found
     */
    private OpenSearchException extractOpenSearchException(Throwable ex) {
        if (ex instanceof OpenSearchException) {
            return (OpenSearchException) ex;
        } else if (ex.getCause() instanceof OpenSearchException) {
            return (OpenSearchException) ex.getCause();
        }
        return null;
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
