/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

/**
 * This class defines custom metric constants used for monitoring flint operations.
 */
public class MetricConstants {

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
     * Metric name for counting the errors encountered with Amazon S3 operations.
     */
    public static final String S3_ERR_CNT_METRIC = "s3.error.count";

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
    public static final String STATEMENT_PROCESSING_TIME_METRIC = "STATEMENT.processingTime";
}