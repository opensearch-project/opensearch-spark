/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.apache.spark.metrics.source.Source;
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

    public static void incrementCounter(String metricName) {
        Counter counter = getOrCreateCounter(metricName);
        if (counter != null) {
            counter.inc();
        }
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
