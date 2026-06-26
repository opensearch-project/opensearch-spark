/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import com.amazonaws.AmazonServiceException;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    static void recordLatency(String metricNamePrefix, long latencyMilliseconds) {
        MetricsUtil.addHistoricGauge(metricNamePrefix + ".processingTime", latencyMilliseconds);
    }

    /**
     * Records the failure of an OpenSearch operation by incrementing the corresponding metric counter.
     * If the exception is an OpenSearchException with a specific status code (e.g., 403),
     * it increments a metric specifically for that status code.
     * Otherwise, it increments a general failure metric counter based on the status code category (e.g., 4xx, 5xx).
     *
     * @param metricNamePrefix the prefix for the metric name which is used to construct the full metric name for failure
     * @param t                the exception encountered during the operation, used to determine the type of failure
     */
    static void recordOperationFailure(String metricNamePrefix, Throwable t) {
        OpenSearchException openSearchException = extractOpenSearchException(t);
        int statusCode;
        if (openSearchException != null) {
            statusCode = openSearchException.status().getStatus();
            CustomLogging.logError(new OperationMessage("OpenSearch Operation failed.", statusCode), openSearchException);
        } else {
            AmazonServiceException amazonServiceException = extractAmazonServiceException(t);
            if (amazonServiceException != null) {
                statusCode = amazonServiceException.getStatusCode();
            } else {
                // Fall back to parsing the status code from the exception message when it is
                // not available on a typed exception. Defaults to 500 if none can be parsed.
                statusCode = extractStatusCodeFromMessage(t);
            }
            CustomLogging.logError(new OperationMessage("OpenSearch Operation failed with an exception.", statusCode), t);
        }
        if (statusCode == 403) {
            String forbiddenErrorMetricName = metricNamePrefix + ".403.count";
            MetricsUtil.incrementCounter(forbiddenErrorMetricName);
        } else if (statusCode == 429) {
            MetricsUtil.incrementCounter(metricNamePrefix + ".429.count");
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

    /**
     * Extracts an AmazonServiceException from the given Throwable.
     * Checks if the Throwable is an instance of AmazonServiceException or caused by one.
     *
     * @param e the exception to be checked
     * @return the extracted AmazonServiceException, or null if not found
     */
    static AmazonServiceException extractAmazonServiceException(Throwable e) {
        if (e instanceof AmazonServiceException) {
            return (AmazonServiceException) e;
        } else if (e.getCause() == null) {
            return null;
        } else {
            return extractAmazonServiceException(e.getCause());
        }
    }

    /** Pattern matching an HTTP status code embedded in an exception message, e.g. "Status Code: 403". */
    Pattern STATUS_CODE_PATTERN = Pattern.compile("Status Code:\\s*(\\d{3})");

    /**
     * Extracts an HTTP status code embedded in the message text of the given Throwable or any of
     * its causes. This handles exceptions that carry the status code only as part of the message
     * string rather than on a typed exception.
     *
     * @param e the exception to be checked
     * @return the parsed status code, or 500 if none can be found in the cause chain
     */
    static int extractStatusCodeFromMessage(Throwable e) {
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                Matcher matcher = STATUS_CODE_PATTERN.matcher(message);
                if (matcher.find()) {
                    return Integer.parseInt(matcher.group(1));
                }
            }
            current = current.getCause();
        }
        return 500;
    }
}
