package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsUtilTest {

    @Test
    public void testIncrementDecrementCounter() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock FlintMetricSource
            FlintMetricSource flintMetricSource = Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME()).head())
                    .thenReturn(flintMetricSource);

            // Test the methods
            String testMetric = "testPrefix.2xx.count";
            MetricsUtil.incrementCounter(testMetric);
            MetricsUtil.incrementCounter(testMetric);
            MetricsUtil.decrementCounter(testMetric);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(3)).metricRegistry();
            Counter counter = flintMetricSource.metricRegistry().getCounters().get(testMetric);
            Assertions.assertNotNull(counter);
            Assertions.assertEquals(counter.getCount(), 1);
        }
    }

    @Test
    public void testStartStopTimer() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock FlintMetricSource
            FlintMetricSource flintMetricSource = Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME()).head())
                    .thenReturn(flintMetricSource);

            // Test the methods
            String testMetric = "testPrefix.processingTime";
            Timer.Context context = MetricsUtil.getTimerContext(testMetric);
            TimeUnit.MILLISECONDS.sleep(500);
            MetricsUtil.stopTimer(context);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(1)).metricRegistry();
            Timer timer = flintMetricSource.metricRegistry().getTimers().get(testMetric);
            Assertions.assertNotNull(timer);
            Assertions.assertEquals(timer.getCount(), 1L);
            assertEquals(1.9, timer.getMeanRate(), 0.1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRegisterGaugeWhenMetricRegistryIsAvailable() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock FlintMetricSource
            FlintMetricSource flintMetricSource = Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME()).head())
                    .thenReturn(flintMetricSource);

            // Setup gauge
            AtomicInteger testValue = new AtomicInteger(1);
            String gaugeName = "test.gauge";
            MetricsUtil.registerGauge(gaugeName, testValue);

            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(1)).metricRegistry();

            Gauge gauge = flintMetricSource.metricRegistry().getGauges().get(gaugeName);
            Assertions.assertNotNull(gauge);
            Assertions.assertEquals(gauge.getValue(), 1);

            testValue.incrementAndGet();
            testValue.incrementAndGet();
            testValue.decrementAndGet();
            Assertions.assertEquals(gauge.getValue(), 2);
        }
    }
}