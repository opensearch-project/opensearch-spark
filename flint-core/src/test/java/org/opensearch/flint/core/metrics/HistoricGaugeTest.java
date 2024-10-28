/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import org.junit.Test;
import static org.junit.Assert.*;
import org.opensearch.flint.core.metrics.HistoricGauge.DataPoint;

import java.util.List;

public class HistoricGaugeTest {

  @Test
  public void testGetValue_EmptyGauge_ShouldReturnNull() {
    HistoricGauge gauge= new HistoricGauge();
    assertNull(gauge.getValue());
  }

  @Test
  public void testGetValue_WithSingleDataPoint_ShouldReturnFirstValue() {
    HistoricGauge gauge= new HistoricGauge();
    Long value = 100L;
    gauge.addDataPoint(value);

    assertEquals(value, gauge.getValue());
  }

  @Test
  public void testGetValue_WithMultipleDataPoints_ShouldReturnFirstValue() {
    HistoricGauge gauge= new HistoricGauge();
    Long firstValue = 100L;
    Long secondValue = 200L;
    gauge.addDataPoint(firstValue);
    gauge.addDataPoint(secondValue);

    assertEquals(firstValue, gauge.getValue());
  }

  @Test
  public void testPollDataPoints_WithMultipleDataPoints_ShouldReturnAndClearDataPoints() {
    HistoricGauge gauge= new HistoricGauge();
    gauge.addDataPoint(100L);
    gauge.addDataPoint(200L);
    gauge.addDataPoint(300L);

    List<DataPoint> dataPoints = gauge.pollDataPoints();

    assertEquals(3, dataPoints.size());
    assertEquals(Long.valueOf(100L), dataPoints.get(0).getValue());
    assertEquals(Long.valueOf(200L), dataPoints.get(1).getValue());
    assertEquals(Long.valueOf(300L), dataPoints.get(2).getValue());

    assertTrue(gauge.pollDataPoints().isEmpty());
  }

  @Test
  public void testAddDataPoint_ShouldAddDataPointWithCorrectValueAndTimestamp() {
    HistoricGauge gauge= new HistoricGauge();
    Long value = 100L;
    gauge.addDataPoint(value);

    List<DataPoint> dataPoints = gauge.pollDataPoints();

    assertEquals(1, dataPoints.size());
    assertEquals(value, dataPoints.get(0).getValue());
    assertTrue(dataPoints.get(0).getTimestamp() > 0);
  }

  @Test
  public void testPollDataPoints_EmptyGauge_ShouldReturnEmptyList() {
    HistoricGauge gauge= new HistoricGauge();
    List<DataPoint> dataPoints = gauge.pollDataPoints();

    assertTrue(dataPoints.isEmpty());
  }
}
