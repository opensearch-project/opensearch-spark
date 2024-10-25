/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.FlintMetricSource;
import org.apache.spark.metrics.source.FlintIndexMetricSource;
import org.apache.spark.metrics.source.Source;
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
        testIncrementDecrementCounterHelper(false);
    }

    @Test
    public void testIncrementDecrementCounterForIndexMetrics() {
        testIncrementDecrementCounterHelper(true);
    }

    private void testIncrementDecrementCounterHelper(boolean isIndexMetric) {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock appropriate MetricSource
            String sourceName = isIndexMetric ? FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME() : FlintMetricSource.FLINT_METRIC_SOURCE_NAME();
            Source metricSource = isIndexMetric ? Mockito.spy(new FlintIndexMetricSource()) : Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(sourceName).head()).thenReturn(metricSource);

            // Test the methods
            String testMetric = "testPrefix.2xx.count";
            MetricsUtil.incrementCounter(testMetric, isIndexMetric);
            MetricsUtil.incrementCounter(testMetric, isIndexMetric);
            MetricsUtil.decrementCounter(testMetric, isIndexMetric);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(metricSource, times(3)).metricRegistry();
            Counter counter = metricSource.metricRegistry().getCounters().get(testMetric);
            Assertions.assertNotNull(counter);
            Assertions.assertEquals(1, counter.getCount());
        }
    }

    @Test
    public void testStartStopTimer() {
        testStartStopTimerHelper(false);
    }

    @Test
    public void testStartStopTimerForIndexMetrics() {
        testStartStopTimerHelper(true);
    }

    private void testStartStopTimerHelper(boolean isIndexMetric) {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock appropriate MetricSource
            String sourceName = isIndexMetric ? FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME() : FlintMetricSource.FLINT_METRIC_SOURCE_NAME();
            Source metricSource = isIndexMetric ? Mockito.spy(new FlintIndexMetricSource()) : Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(sourceName).head()).thenReturn(metricSource);

            // Test the methods
            String testMetric = "testPrefix.processingTime";
            Timer.Context context = MetricsUtil.getTimerContext(testMetric, isIndexMetric);
            TimeUnit.MILLISECONDS.sleep(500);
            MetricsUtil.stopTimer(context);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(metricSource, times(1)).metricRegistry();
            Timer timer = metricSource.metricRegistry().getTimers().get(testMetric);
            Assertions.assertNotNull(timer);
            Assertions.assertEquals(1L, timer.getCount());
            assertEquals(1.9, timer.getMeanRate(), 0.1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetTimer() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock appropriate MetricSource
            String sourceName = FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME();
            Source metricSource = Mockito.spy(new FlintIndexMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(sourceName).head()).thenReturn(
                metricSource);

            // Test the methods
            String testMetric = "testPrefix.processingTime";
            long duration = 500;
            MetricsUtil.getTimer(testMetric, true).update(duration, TimeUnit.MILLISECONDS);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(metricSource, times(1)).metricRegistry();
            Timer timer = metricSource.metricRegistry().getTimers().get(testMetric);
            Assertions.assertNotNull(timer);
            Assertions.assertEquals(1L, timer.getCount());
            assertEquals(Duration.ofMillis(duration).getNano(), timer.getSnapshot().getMean(), 0.1);
        }
    }

    @Test
    public void testRegisterGauge() {
        testRegisterGaugeHelper(false);
    }

    @Test
    public void testRegisterGaugeForIndexMetrics() {
        testRegisterGaugeHelper(true);
    }

    private void testRegisterGaugeHelper(boolean isIndexMetric) {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock appropriate MetricSource
            String sourceName = isIndexMetric ? FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME() : FlintMetricSource.FLINT_METRIC_SOURCE_NAME();
            Source metricSource = isIndexMetric ? Mockito.spy(new FlintIndexMetricSource()) : Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(sourceName).head()).thenReturn(metricSource);

            // Setup gauge
            AtomicInteger testValue = new AtomicInteger(1);
            String gaugeName = "test.gauge";
            MetricsUtil.registerGauge(gaugeName, testValue, isIndexMetric);

            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(metricSource, times(1)).metricRegistry();

            Gauge gauge = metricSource.metricRegistry().getGauges().get(gaugeName);
            Assertions.assertNotNull(gauge);
            Assertions.assertEquals(1, gauge.getValue());

            testValue.incrementAndGet();
            testValue.incrementAndGet();
            testValue.decrementAndGet();
            Assertions.assertEquals(2, gauge.getValue());
        }
    }

    @Test
    public void testDefaultBehavior() {
        try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
            // Mock SparkEnv
            SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
            sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

            // Mock FlintMetricSource
            FlintMetricSource flintMetricSource = Mockito.spy(new FlintMetricSource());
            when(sparkEnv.metricsSystem().getSourcesByName(FlintMetricSource.FLINT_METRIC_SOURCE_NAME()).head())
                    .thenReturn(flintMetricSource);

            // Test default behavior (non-index metrics)
            String testCountMetric = "testDefault.count";
            String testTimerMetric = "testDefault.time";
            String testGaugeMetric = "testDefault.gauge";
            MetricsUtil.incrementCounter(testCountMetric);
            MetricsUtil.getTimerContext(testTimerMetric);
            MetricsUtil.registerGauge(testGaugeMetric, new AtomicInteger(0), false);

            // Verify interactions
            verify(sparkEnv.metricsSystem(), times(0)).registerSource(any());
            verify(flintMetricSource, times(3)).metricRegistry();
            Assertions.assertNotNull(flintMetricSource.metricRegistry().getCounters().get(testCountMetric));
            Assertions.assertNotNull(flintMetricSource.metricRegistry().getTimers().get(testTimerMetric));
            Assertions.assertNotNull(flintMetricSource.metricRegistry().getGauges().get(testGaugeMetric));
        }
    }
}
