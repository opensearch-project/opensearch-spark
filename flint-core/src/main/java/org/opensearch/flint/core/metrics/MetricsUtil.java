/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.apache.spark.metrics.source.Source;
import org.opensearch.OpenSearchException;
import scala.collection.Seq;

import java.util.logging.Logger;

/**
 * Utility class for managing metrics in the OpenSearch Flint context.
 */
public final class MetricsUtil {

    private static final Logger LOG = Logger.getLogger(MetricsUtil.class.getName());

    // Private constructor to prevent instantiation
    private MetricsUtil() {
    }

    /**
     * Publish an OpenSearch metric based on the status code.
     *
     * @param metricNamePrefix the prefix for the metric name
     * @param statusCode       the HTTP status code
     */
    public static void publishOpenSearchMetric(String metricNamePrefix, int statusCode) {
        String metricName = constructMetricName(metricNamePrefix, statusCode);
        Counter counter = getOrCreateCounter(metricName);
        if (counter != null) {
            counter.inc();
        }
    }

    // Constructs the metric name based on the provided prefix and status code
    private static String constructMetricName(String metricNamePrefix, int statusCode) {
        String metricSuffix = getMetricSuffixForStatusCode(statusCode);
        return metricNamePrefix + "." + metricSuffix;
    }

    // Determines the metric suffix based on the HTTP status code
    private static String getMetricSuffixForStatusCode(int statusCode) {
        if (statusCode == 403) {
            return "403.count";
        } else if (statusCode >= 500) {
            return "5xx.count";
        } else if (statusCode >= 400) {
            return "4xx.count";
        } else if (statusCode >= 200) {
            return "2xx.count";
        }
        return "unknown.count"; // default for unhandled status codes
    }

    // Retrieves or creates a new counter for the given metric name
    private static Counter getOrCreateCounter(String metricName) {
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null) {
            LOG.warning("Spark environment not available, cannot instrument metric: " + metricName);
            return null;
        }

        FlintMetricSource flintMetricSource = getOrInitFlintMetricSource(sparkEnv);
        Counter counter = flintMetricSource.metricRegistry().getCounters().get(metricName);
        if (counter == null) {
            counter = flintMetricSource.metricRegistry().counter(metricName);
        }
        return counter;
    }

    // Gets or initializes the FlintMetricSource
    private static FlintMetricSource getOrInitFlintMetricSource(SparkEnv sparkEnv) {
        Seq<Source> metricSourceSeq = sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME());

        if (metricSourceSeq == null || metricSourceSeq.isEmpty()) {
            FlintMetricSource metricSource = new FlintMetricSource();
            sparkEnv.metricsSystem().registerSource(metricSource);
            return metricSource;
        }
        return (FlintMetricSource) metricSourceSeq.head();
    }
}
