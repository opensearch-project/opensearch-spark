/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
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

    /**
     * Increments the Counter metric associated with the given metric name.
     * If the counter does not exist, it is created before being incremented.
     *
     * @param metricName The name of the metric for which the counter is incremented.
     *                   This name is used to retrieve or create the counter.
     */
    public static void incrementCounter(String metricName) {
        Counter counter = getOrCreateCounter(metricName);
        if (counter != null) {
            counter.inc();
        }
    }

    /**
     * Decrements the value of the specified metric counter by one, if the counter exists and its current count is greater than zero.
     *
     * @param metricName The name of the metric counter to be decremented.
     */
    public static void decrementCounter(String metricName) {
        Counter counter = getOrCreateCounter(metricName);
        if (counter != null && counter.getCount() > 0) {
            counter.dec();
        }
    }

    /**
     * Retrieves a {@link Timer.Context} for the specified metric name, creating a new timer if one does not already exist.
     * This context can be used to measure the duration of a particular operation or event.
     *
     * @param metricName The name of the metric timer to retrieve the context for.
     * @return A {@link Timer.Context} instance for timing operations, or {@code null} if the timer could not be created or retrieved.
     */
    public static Timer.Context getTimerContext(String metricName) {
        Timer timer = getOrCreateTimer(metricName);
        if (timer != null) {
            return timer.time();
        }
        return null;
    }

    /**
     * Stops the timer associated with the given {@link Timer.Context}, effectively recording the elapsed time since the timer was started
     * and returning the duration. If the context is {@code null}, this method does nothing and returns {@code null}.
     *
     * @param context The {@link Timer.Context} to stop. May be {@code null}, in which case this method has no effect and returns {@code null}.
     * @return The elapsed time in nanoseconds since the timer was started, or {@code null} if the context was {@code null}.
     */
    public static Long stopTimer(Timer.Context context) {
        if (context != null) {
            return context.stop();
        }
        return null;
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

    // Retrieves or creates a new Timer for the given metric name
    private static Timer getOrCreateTimer(String metricName) {
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null) {
            LOG.warning("Spark environment not available, cannot instrument metric: " + metricName);
            return null;
        }

        FlintMetricSource flintMetricSource = getOrInitFlintMetricSource(sparkEnv);
        Timer timer = flintMetricSource.metricRegistry().getTimers().get(metricName);
        if (timer == null) {
            timer = flintMetricSource.metricRegistry().timer(metricName);
        }
        return timer;
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
