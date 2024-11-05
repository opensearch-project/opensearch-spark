/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.function.Supplier;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.apache.spark.metrics.source.FlintIndexMetricSource;
import org.apache.spark.metrics.source.Source;
import scala.collection.Seq;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Utility class for managing metrics in the OpenSearch Flint context.
 */
public final class MetricsUtil {

    private static final Logger LOG = Logger.getLogger(MetricsUtil.class.getName());

    private MetricsUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * Increments the Counter metric associated with the given metric name.
     * If the counter does not exist, it is created before being incremented.
     *
     * @param metricName The name of the metric for which the counter is incremented.
     */
    public static void incrementCounter(String metricName) {
        incrementCounter(metricName, false);
    }

    /**
     * Increments the Counter metric associated with the given metric name.
     * If the counter does not exist, it is created before being incremented.
     *
     * @param metricName The name of the metric for which the counter is incremented.
     * @param isIndexMetric Whether this metric is an index-specific metric.
     */
    public static void incrementCounter(String metricName, boolean isIndexMetric) {
        Counter counter = getOrCreateCounter(metricName, isIndexMetric);
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
        decrementCounter(metricName, false);
    }

    /**
     * Decrements the value of the specified metric counter by one, if the counter exists and its current count is greater than zero.
     *
     * @param metricName The name of the metric counter to be decremented.
     * @param isIndexMetric Whether this metric is an index-specific metric.
     */
    public static void decrementCounter(String metricName, boolean isIndexMetric) {
        Counter counter = getOrCreateCounter(metricName, isIndexMetric);
        if (counter != null && counter.getCount() > 0) {
            counter.dec();
        }
    }

    public static void setCounter(String metricName, boolean isIndexMetric, long n) {
        Counter counter = getOrCreateCounter(metricName, isIndexMetric);
        if (counter != null) {
            counter.dec(counter.getCount());
            counter.inc(n);
            LOG.info("counter: " + counter.getCount());
        }
    }

    /**
     * Retrieves a {@link Timer.Context} for the specified metric name, creating a new timer if one does not already exist.
     *
     * @param metricName The name of the metric timer to retrieve the context for.
     * @return A {@link Timer.Context} instance for timing operations, or {@code null} if the timer could not be created or retrieved.
     */
    public static Timer.Context getTimerContext(String metricName) {
        return getTimerContext(metricName, false);
    }

    /**
     * Retrieves a {@link Timer.Context} for the specified metric name, creating a new timer if one does not already exist.
     *
     * @param metricName The name of the metric timer to retrieve the context for.
     * @param isIndexMetric Whether this metric is an index-specific metric.
     * @return A {@link Timer.Context} instance for timing operations, or {@code null} if the timer could not be created or retrieved.
     */
    public static Timer.Context getTimerContext(String metricName, boolean isIndexMetric) {
        Timer timer = getOrCreateTimer(metricName, isIndexMetric);
        return timer != null ? timer.time() : null;
    }

    /**
     * Stops the timer associated with the given {@link Timer.Context}.
     *
     * @param context The {@link Timer.Context} to stop. May be {@code null}.
     * @return The elapsed time in nanoseconds since the timer was started, or {@code null} if the context was {@code null}.
     */
    public static Long stopTimer(Timer.Context context) {
        return context != null ? context.stop() : null;
    }

    public static Timer getTimer(String metricName, boolean isIndexMetric) {
        return getOrCreateTimer(metricName, isIndexMetric);
    }

    /**
     * Registers a HistoricGauge metric with the provided name and value.
     *
     * @param metricName The name of the HistoricGauge metric to register.
     * @param value The value to be stored
     */
    public static void addHistoricGauge(String metricName, final long value) {
        HistoricGauge historicGauge = getOrCreateHistoricGauge(metricName);
        if (historicGauge != null) {
            historicGauge.addDataPoint(value);
        }
    }

    /**
     * Automatically emit latency metric as Historic Gauge for the execution of supplier
     * @param supplier the lambda to be metered
     * @param metricName name of the metric
     * @return value returned by supplier
     */
    public static <T> T withLatencyAsHistoricGauge(Supplier<T> supplier, String metricName) {
        long startTime = System.currentTimeMillis();
        try {
            return supplier.get();
        } finally {
            addHistoricGauge(metricName, System.currentTimeMillis() - startTime);
        }
    }

    private static HistoricGauge getOrCreateHistoricGauge(String metricName) {
        MetricRegistry metricRegistry = getMetricRegistry(false);
        return metricRegistry != null ? metricRegistry.gauge(metricName, HistoricGauge::new) : null;
    }

    /**
     * Registers a gauge metric with the provided name and value.
     *
     * @param metricName The name of the gauge metric to register.
     * @param value The AtomicInteger whose current value should be reflected by the gauge.
     */
    public static void registerGauge(String metricName, final AtomicInteger value) {
        registerGauge(metricName, value, false);
    }

    /**
     * Registers a gauge metric with the provided name and value.
     *
     * @param metricName The name of the gauge metric to register.
     * @param value The AtomicInteger whose current value should be reflected by the gauge.
     * @param isIndexMetric Whether this metric is an index-specific metric.
     */
    public static void registerGauge(String metricName, final AtomicInteger value, boolean isIndexMetric) {
        MetricRegistry metricRegistry = getMetricRegistry(isIndexMetric);
        if (metricRegistry == null) {
            LOG.warning("MetricRegistry not available, cannot register gauge: " + metricName);
            return;
        }
        metricRegistry.register(metricName, (Gauge<Integer>) value::get);
    }

    private static Counter getOrCreateCounter(String metricName, boolean isIndexMetric) {
        MetricRegistry metricRegistry = getMetricRegistry(isIndexMetric);
        return metricRegistry != null ? metricRegistry.counter(metricName) : null;
    }

    private static Timer getOrCreateTimer(String metricName, boolean isIndexMetric) {
        MetricRegistry metricRegistry = getMetricRegistry(isIndexMetric);
        return metricRegistry != null ? metricRegistry.timer(metricName) : null;
    }

    private static MetricRegistry getMetricRegistry(boolean isIndexMetric) {
        SparkEnv sparkEnv = SparkEnv.get();
        if (sparkEnv == null) {
            LOG.warning("Spark environment not available, cannot access MetricRegistry.");
            return null;
        }

        Source metricSource = isIndexMetric ? 
            getOrInitMetricSource(sparkEnv, FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME(), FlintIndexMetricSource::new) :
            getOrInitMetricSource(sparkEnv, FlintMetricSource.FLINT_METRIC_SOURCE_NAME(), FlintMetricSource::new);
        return metricSource.metricRegistry();
    }

    private static Source getOrInitMetricSource(SparkEnv sparkEnv, String sourceName, java.util.function.Supplier<Source> sourceSupplier) {
        Seq<Source> metricSourceSeq = sparkEnv.metricsSystem().getSourcesByName(sourceName);

        if (metricSourceSeq == null || metricSourceSeq.isEmpty()) {
            Source metricSource = sourceSupplier.get();
            sparkEnv.metricsSystem().registerSource(metricSource);
            return metricSource;
        }
        return metricSourceSeq.head();
    }
}
