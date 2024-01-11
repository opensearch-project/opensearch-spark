package org.opensearch.flint.core.metrics.aop;

import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.opensearch.OpenSearchException;
import org.opensearch.rest.RestStatus;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MetricsAspectTest {
    private MetricsAspect metricsAspect;

    @Test
    public void testLogSuccess() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            metricsAspect = new MetricsAspect();

            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);
            FlintMetricSource flintMetricSource = spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(flintMetricSource.sourceName()).head())
                    .thenReturn(flintMetricSource);

            PublishMetrics publishMetricsAnnotation = mock(PublishMetrics.class);
            when(publishMetricsAnnotation.metricNamePrefix()).thenReturn("testMetric");

            String expectedMetricName = "testMetric.2xx.count";
            String actualMetricName = metricsAspect.logSuccess(publishMetricsAnnotation);

            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(2)).metricRegistry();
            assertEquals(expectedMetricName, actualMetricName);
            assertEquals(1, flintMetricSource.metricRegistry().getCounters().get(expectedMetricName).getCount());
        }
    }

    @Test
    public void testLogExceptionWithOpenSearchException() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            metricsAspect = new MetricsAspect();

            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);
            FlintMetricSource flintMetricSource = spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(flintMetricSource.sourceName()).head())
                    .thenReturn(flintMetricSource);

            PublishMetrics publishMetricsAnnotation = mock(PublishMetrics.class);
            when(publishMetricsAnnotation.metricNamePrefix()).thenReturn("testMetric");

            OpenSearchException exception = mock(OpenSearchException.class);
            when(exception.getMessage()).thenReturn("Error");
            when(exception.getCause()).thenReturn(new RuntimeException());
            when(exception.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);

            String expectedMetricName = "testMetric.5xx.count";
            String actualMetricName = metricsAspect.logException(exception, publishMetricsAnnotation);

            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(2)).metricRegistry();
            assertEquals(expectedMetricName, actualMetricName);
            assertEquals(1, flintMetricSource.metricRegistry().getCounters().get(expectedMetricName).getCount());
        }
    }
}
