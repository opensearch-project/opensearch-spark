package org.opensearch.flint.core.metrics;

import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsUtilTest {

    @Test
    public void incOpenSearchMetric() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock FlintMetricSource
            FlintMetricSource flintMetricSource = Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME()).head())
                    .thenReturn(flintMetricSource);

            // Test the method
            MetricsUtil.publishOpenSearchMetric("testPrefix", 200);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(2)).metricRegistry();
            Assertions.assertNotNull(
                    flintMetricSource.metricRegistry().getCounters().get("testPrefix.2xx.count"));
        }
    }
}