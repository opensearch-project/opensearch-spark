/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.util.Arrays;
import java.util.function.Consumer;
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
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import java.io.IOException;
import org.opensearch.flint.core.storage.OpenSearchBulkWrapper;

import static org.opensearch.flint.core.metrics.MetricConstants.OS_BULK_OP_METRIC_PREFIX;
import static org.opensearch.flint.core.metrics.MetricConstants.OS_CREATE_OP_METRIC_PREFIX;
import static org.opensearch.flint.core.metrics.MetricConstants.OS_READ_OP_METRIC_PREFIX;
import static org.opensearch.flint.core.metrics.MetricConstants.OS_SEARCH_OP_METRIC_PREFIX;
import static org.opensearch.flint.core.metrics.MetricConstants.OS_WRITE_OP_METRIC_PREFIX;

/**
 * A wrapper class for RestHighLevelClient to facilitate OpenSearch operations
 * with integrated metrics tracking.
 */
public class RestHighLevelClientWrapper implements IRestHighLevelClient {
    private final RestHighLevelClient client;
    private final OpenSearchBulkWrapper bulkRetryWrapper;

    private final static JacksonJsonpMapper JACKSON_MAPPER = new JacksonJsonpMapper();

    /**
     * Constructs a new RestHighLevelClientWrapper.
     *
     * @param client the RestHighLevelClient instance to wrap
     */
    public RestHighLevelClientWrapper(RestHighLevelClient client, OpenSearchBulkWrapper bulkRetryWrapper) {
        this.client = client;
        this.bulkRetryWrapper = bulkRetryWrapper;
    }

    @Override
    public BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return execute(() -> bulkRetryWrapper.bulk(client, bulkRequest, options),
            OS_WRITE_OP_METRIC_PREFIX, OS_BULK_OP_METRIC_PREFIX);
    }

    @Override
    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException {
        return execute(() -> client.clearScroll(clearScrollRequest, options),
            OS_READ_OP_METRIC_PREFIX);
    }

    @Override
    public CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest, RequestOptions options) throws IOException {
        return execute(() -> client.indices().create(createIndexRequest, options),
            OS_WRITE_OP_METRIC_PREFIX, OS_CREATE_OP_METRIC_PREFIX);
    }

    @Override
    public void updateIndexMapping(PutMappingRequest putMappingRequest, RequestOptions options) throws IOException {
        execute(() -> client.indices().putMapping(putMappingRequest, options),
            OS_WRITE_OP_METRIC_PREFIX);
    }

    @Override
    public void deleteIndex(DeleteIndexRequest deleteIndexRequest, RequestOptions options) throws IOException {
        execute(() -> client.indices().delete(deleteIndexRequest, options),
            OS_WRITE_OP_METRIC_PREFIX);
    }

    @Override
    public DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException {
        return execute(() -> client.delete(deleteRequest, options), OS_WRITE_OP_METRIC_PREFIX);
    }

    @Override
    public GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException {
        return execute(() -> client.get(getRequest, options), OS_READ_OP_METRIC_PREFIX);
    }

    @Override
    public GetIndexResponse getIndex(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
        return execute(() -> client.indices().get(getIndexRequest, options),
            OS_READ_OP_METRIC_PREFIX);
    }

    @Override
    public IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException {
        return execute(() -> client.index(indexRequest, options), OS_WRITE_OP_METRIC_PREFIX);
    }

    @Override
    public Boolean doesIndexExist(GetIndexRequest getIndexRequest, RequestOptions options) throws IOException {
        return execute(() -> client.indices().exists(getIndexRequest, options),
            OS_READ_OP_METRIC_PREFIX);
    }

    @Override
    public SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return execute(() -> client.search(searchRequest, options), OS_READ_OP_METRIC_PREFIX, OS_SEARCH_OP_METRIC_PREFIX);
    }

    @Override
    public SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return execute(() -> client.scroll(searchScrollRequest, options), OS_READ_OP_METRIC_PREFIX);
    }

    @Override
    public UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException {
        return execute(() -> client.update(updateRequest, options), OS_WRITE_OP_METRIC_PREFIX);
    }

    @Override
    public IndicesStatsResponse stats(IndicesStatsRequest request) throws IOException {
        return execute(() -> {
              OpenSearchClient openSearchClient =
                  new OpenSearchClient(new RestClientTransport(client.getLowLevelClient(),
                      new JacksonJsonpMapper()));
              return openSearchClient.indices().stats(request);
            }, OS_READ_OP_METRIC_PREFIX
        );
    }

    @Override
    public CreatePitResponse createPit(CreatePitRequest request) throws IOException {
        return execute(() -> openSearchClient().createPit(request), OS_WRITE_OP_METRIC_PREFIX);
    }

    /**
     * Executes a given operation, tracks metrics, and handles exceptions.
     *
     * @param <T>              the return type of the operation
     * @param operation        the operation to execute
     * @param metricNamePrefixes array of prefixes for the metric name
     * @return the result of the operation
     * @throws IOException if an I/O exception occurs
     */
    private <T> T execute(IOCallable<T> operation, String... metricNamePrefixes) throws IOException {
        long startTime = System.currentTimeMillis();
        try {
            T result = operation.call();
            eachPrefix(IRestHighLevelClient::recordOperationSuccess, metricNamePrefixes);
            return result;
        } catch (Exception e) {
            eachPrefix(prefix -> IRestHighLevelClient.recordOperationFailure(prefix, e), metricNamePrefixes);
            throw e;
        } finally {
            long latency = System.currentTimeMillis() - startTime;
            eachPrefix(prefix -> IRestHighLevelClient.recordLatency(prefix, latency), metricNamePrefixes);
        }
    }

    private static void eachPrefix(Consumer<String> fn, String... prefixes) {
        Arrays.stream(prefixes).forEach(fn);
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

    private OpenSearchClient openSearchClient() {
        return new OpenSearchClient(new RestClientTransport(client.getLowLevelClient(),JACKSON_MAPPER
        ));
    }
}
