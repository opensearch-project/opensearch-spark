/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.Source;
import org.mockito.MockedStatic;
import org.opensearch.flint.core.metrics.HistoricGauge.DataPoint;

/**
 * Utility class for verifying metrics
 */
public class MetricsTestUtil {
  @AllArgsConstructor
  public static class MetricsVerifier {

    final MetricRegistry metricRegistry;

    public void assertMetricExist(String metricName) {
      assertNotNull(metricRegistry.getMetrics().get(metricName));
    }

    public void assertMetricClass(String metricName, Class<?> clazz) {
      assertMetricExist(metricName);
      assertEquals(clazz, metricRegistry.getMetrics().get(metricName).getClass());
    }

    public void assertHistoricGauge(String metricName, long... values) {
      HistoricGauge historicGauge = getHistoricGauge(metricName);
      List<DataPoint> dataPoints = historicGauge.getDataPoints();
      for (int i = 0; i < values.length; i++) {
        assertEquals(values[i], dataPoints.get(i).getValue().longValue());
      }
    }

    private HistoricGauge getHistoricGauge(String metricName) {
      assertMetricClass(metricName, HistoricGauge.class);
      return (HistoricGauge) metricRegistry.getMetrics().get(metricName);
    }

    public void assertMetricNotExist(String metricName) {
      assertNull(metricRegistry.getMetrics().get(metricName));
    }
  }

  @FunctionalInterface
  public interface ThrowableConsumer<T> {
    void accept(T t) throws Exception;
  }

  public static void withMetricEnv(ThrowableConsumer<MetricsVerifier> test) throws Exception {
    try (MockedStatic<SparkEnv> sparkEnvMock = mockStatic(SparkEnv.class)) {
      SparkEnv sparkEnv = mock(SparkEnv.class, RETURNS_DEEP_STUBS);
      sparkEnvMock.when(SparkEnv::get).thenReturn(sparkEnv);

      Source metricSource = mock(Source.class);
      MetricRegistry metricRegistry = new MetricRegistry();
      when(metricSource.metricRegistry()).thenReturn(metricRegistry);
      when(sparkEnv.metricsSystem().getSourcesByName(any()).head()).thenReturn(metricSource);

      test.accept(new MetricsVerifier(metricRegistry));
    }
  }
}
