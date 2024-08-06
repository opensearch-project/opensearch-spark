/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
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
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;
import org.opensearch.flint.core.logging.CustomLogging;
import org.opensearch.flint.core.logging.OperationMessage;
import org.opensearch.flint.core.metrics.MetricsUtil;

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

    void updateIndexMapping(PutMappingRequest putMappingRequest, RequestOptions options) throws IOException;

    void deleteIndex(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException;

    DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException;

    GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException;

    GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException;

    IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException;

    Boolean doesIndexExist(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException;

    SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException;

    SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException;

    DocWriteResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException;

    IndicesStatsResponse stats(IndicesStatsRequest request) throws IOException;

    CreatePitResponse createPit(CreatePitRequest request) throws IOException;

    /**
     * Records the success of an OpenSearch operation by incrementing the corresponding metric counter.
     * This method constructs the metric name by appending ".200.count" to the provided metric name prefix.
     * The metric name is then used to increment the counter, indicating a successful operation.
     *
     * @param metricNamePrefix the prefix for the metric name which is used to construct the full metric name for success
     */
    static void recordOperationSuccess(String metricNamePrefix) {
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
    static void recordOperationFailure(String metricNamePrefix, Exception e) {
        OpenSearchException openSearchException = extractOpenSearchException(e);
        int statusCode = openSearchException != null ? openSearchException.status().getStatus() : 500;
        if (openSearchException != null) {
            CustomLogging.logError(new OperationMessage("OpenSearch Operation failed.", statusCode), openSearchException);
        } else {
            CustomLogging.logError("OpenSearch Operation failed with an exception.", e);
        }
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
     * @param e the exception to be checked
     * @return the extracted OpenSearchException, or null if not found
     */
    static OpenSearchException extractOpenSearchException(Throwable e) {
        if (e instanceof OpenSearchException) {
            return (OpenSearchException) e;
        } else if (e.getCause() == null) {
            return null;
        } else {
            return extractOpenSearchException(e.getCause());
        }
    }
}
