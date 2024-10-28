/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

/**
 * This class defines custom metric constants used for monitoring flint operations.
 */
public final class MetricConstants {

    /**
     * The prefix for all read-related metrics in OpenSearch.
     * This constant is used as a part of metric names to categorize and identify metrics related to read operations.
     */
    public static final String OS_READ_OP_METRIC_PREFIX = "opensearch.read";

    /**
     * The prefix for all write-related metrics in OpenSearch.
     * Similar to OS_READ_METRIC_PREFIX, this constant is used for categorizing and identifying metrics that pertain to write operations.
     */
    public static final String OS_WRITE_OP_METRIC_PREFIX = "opensearch.write";

    /**
     * Prefixes for OpenSearch API specific metrics
     */
    public static final String OS_CREATE_OP_METRIC_PREFIX = "opensearch.create";
    public static final String OS_SEARCH_OP_METRIC_PREFIX = "opensearch.search";
    public static final String OS_BULK_OP_METRIC_PREFIX = "opensearch.bulk";

    /**
     * Metric name for request size of opensearch bulk request
     */
    public static final String OPENSEARCH_BULK_SIZE_METRIC = "opensearch.bulk.size.count";

    /**
     * Metric name for opensearch bulk request retry count
     */
    public static final String OPENSEARCH_BULK_RETRY_COUNT_METRIC = "opensearch.bulk.retry.count";
    public static final String OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC = "opensearch.bulk.allRetryFailed.count";

    /**
     * Metric name for counting the errors encountered with Amazon S3 operations.
     */
    public static final String S3_ERR_CNT_METRIC = "s3.error.count";

    /**
     * Metric name for counting the errors encountered with Amazon Glue operations.
     */
    public static final String GLUE_ERR_CNT_METRIC = "glue.error.count";

    /**
     * Metric name for counting the number of sessions currently running.
     */
    public static final String REPL_RUNNING_METRIC = "session.running.count";

    /**
     * Metric name for counting the number of sessions that have failed.
     */
    public static final String REPL_FAILED_METRIC = "session.failed.count";

    /**
     * Metric name for counting the number of sessions that have successfully completed.
     */
    public static final String REPL_SUCCESS_METRIC = "session.success.count";

    /**
     * Metric name for tracking the processing time of sessions.
     */
    public static final String REPL_PROCESSING_TIME_METRIC = "session.processingTime";

    /**
     * Prefix for metrics related to the request metadata read operations.
     */
    public static final String REQUEST_METADATA_READ_METRIC_PREFIX = "request.metadata.read";

    /**
     * Prefix for metrics related to the request metadata write operations.
     */
    public static final String REQUEST_METADATA_WRITE_METRIC_PREFIX = "request.metadata.write";

    /**
     * Metric name for counting failed heartbeat operations on request metadata.
     */
    public static final String REQUEST_METADATA_HEARTBEAT_FAILED_METRIC = "request.metadata.heartbeat.failed.count";

    /**
     * Prefix for metrics related to the result metadata write operations.
     */
    public static final String RESULT_METADATA_WRITE_METRIC_PREFIX = "result.metadata.write";

    /**
     * Metric name for counting the number of statements currently running.
     */
    public static final String STATEMENT_RUNNING_METRIC = "statement.running.count";

    /**
     * Metric name for counting the number of statements that have failed.
     */
    public static final String STATEMENT_FAILED_METRIC = "statement.failed.count";

    /**
     * Metric name for counting the number of statements that have successfully completed.
     */
    public static final String STATEMENT_SUCCESS_METRIC = "statement.success.count";

    /**
     * Metric name for tracking the processing time of statements.
     */
    public static final String STATEMENT_PROCESSING_TIME_METRIC = "statement.processingTime";

    /**
     * Metric for tracking the count of currently running streaming jobs.
     */
    public static final String STREAMING_RUNNING_METRIC = "streaming.running.count";

    /**
     * Metric for tracking the count of streaming jobs that have failed.
     */
    public static final String STREAMING_FAILED_METRIC = "streaming.failed.count";

    /**
     * Metric for tracking the count of streaming jobs that have completed successfully.
     */
    public static final String STREAMING_SUCCESS_METRIC = "streaming.success.count";

    /**
     * Metric for tracking the count of failed heartbeat signals in streaming jobs.
     */
    public static final String STREAMING_HEARTBEAT_FAILED_METRIC = "streaming.heartbeat.failed.count";

    /**
     * Metric for tracking the latency of query execution (start to complete query execution) excluding result write.
     */
    public static final String QUERY_EXECUTION_TIME_METRIC = "query.execution.processingTime";

    /**
     * Metric for tracking the total bytes read from input
     */
    public static final String INPUT_TOTAL_BYTES_READ = "input.totalBytesRead.count";

    /**
     * Metric for tracking the total records read from input
     */
    public static final String INPUT_TOTAL_RECORDS_READ = "input.totalRecordsRead.count";

    /**
     * Metric for tracking the total bytes written to output
     */
    public static final String OUTPUT_TOTAL_BYTES_WRITTEN = "output.totalBytesWritten.count";

    /**
     * Metric for tracking the total records written to output
     */
    public static final String OUTPUT_TOTAL_RECORDS_WRITTEN = "output.totalRecordsWritten.count";

    /**
     * Metric for tracking the latency of checkpoint deletion
     */
    public static final String CHECKPOINT_DELETE_TIME_METRIC = "checkpoint.delete.processingTime";

    private MetricConstants() {
        // Private constructor to prevent instantiation
    }
}
