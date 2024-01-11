/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics.aop;

import com.codahale.metrics.Counter;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.apache.spark.metrics.source.Source;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.opensearch.OpenSearchException;
import scala.collection.Seq;

/**
 * Aspect for logging metrics based on the annotated methods in Flint components.
 * It utilizes AspectJ for intercepting method calls that are annotated with {@link PublishMetrics}
 * to log successful executions and exceptions as metrics.
 */
@Aspect
public class MetricsAspect {

    /**
     * Pointcut to match methods annotated with {@link PublishMetrics}.
     *
     * @param publishMetricsAnnotation the PublishMetrics annotation
     */
    @Pointcut("@annotation(publishMetricsAnnotation)")
    public void annotatedWithPublishMetrics(PublishMetrics publishMetricsAnnotation) {}

    /**
     * After returning advice that logs successful method executions.
     * This method is invoked after a method annotated with {@link PublishMetrics} successfully returns.
     * It publishes a success metric with a standard status code of 200.
     *
     * @param publishMetricsAnnotation the PublishMetrics annotation
     * @return the full metric name for the successful execution
     */
    @AfterReturning(pointcut = "annotatedWithPublishMetrics(publishMetricsAnnotation)", argNames = "publishMetricsAnnotation")
    public String logSuccess(PublishMetrics publishMetricsAnnotation) {
        int statusCode = 200; // Assume success with a status code of 200
        String metricNamePrefix = publishMetricsAnnotation.metricNamePrefix();
        return publishStatusMetrics(metricNamePrefix, statusCode);
    }

    /**
     * After throwing advice that logs exceptions thrown by methods.
     * This method is invoked when a method annotated with {@link PublishMetrics} throws an exception.
     * It extracts the status code from the OpenSearchException and publishes a corresponding metric.
     *
     * @param ex the exception thrown by the method
     * @param publishMetricsAnnotation the PublishMetrics annotation
     * @return the full metric name for the exception, or null if the exception is not an OpenSearchException
     */
    @AfterThrowing(pointcut = "annotatedWithPublishMetrics(publishMetricsAnnotation)", throwing = "ex", argNames = "ex,publishMetricsAnnotation")
    public String logException(Throwable ex, PublishMetrics publishMetricsAnnotation) {
        OpenSearchException openSearchException = extractOpenSearchException(ex);
        if (openSearchException != null) {
            int statusCode = openSearchException.status().getStatus();
            String metricNamePrefix = publishMetricsAnnotation.metricNamePrefix();
            return publishStatusMetrics(metricNamePrefix, statusCode);
        }
        return null;
    }

    /**
     * Extracts an OpenSearchException from the given Throwable.
     * This method checks if the Throwable is an instance of OpenSearchException or caused by one.
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


    private String publishStatusMetrics(String metricNamePrefix, int statusCode) {
        // TODO: Refactor this impl
        String metricName = null;
        if (SparkEnv.get() != null) {
            FlintMetricSource flintMetricSource = getOrInitFlintMetricSource();
            metricName = constructMetricName(metricNamePrefix, statusCode);
            Counter counter = flintMetricSource.metricRegistry().getCounters().get(metricName);
            if (counter == null) {
                counter = flintMetricSource.metricRegistry().counter(metricName);
            }
            counter.inc();
        }
        return metricName;
    }

    private String constructMetricName(String metricNamePrefix, int statusCode) {
        String metricSuffix = null;

        if (statusCode == 200) {
            metricSuffix = "2xx.count";
        } else if (statusCode == 403) {
            metricSuffix = "403.count";
        } else if (statusCode >= 500) {
            metricSuffix = "5xx.count";
        } else if (statusCode >= 400) {
            metricSuffix = "4xx.count";
        }

        return metricNamePrefix + "." + metricSuffix;
    }


    private static FlintMetricSource getOrInitFlintMetricSource() {
        Seq<Source> metricSourceSeq = SparkEnv.get().metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME());

        if (metricSourceSeq == null || metricSourceSeq.isEmpty()) {
            FlintMetricSource metricSource = new FlintMetricSource();
            SparkEnv.get().metricsSystem().registerSource(metricSource);
            return metricSource;
        }
        return (FlintMetricSource) metricSourceSeq.head();
    }
}
