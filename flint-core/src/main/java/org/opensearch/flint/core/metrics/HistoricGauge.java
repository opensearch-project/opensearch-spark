/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

import com.codahale.metrics.Gauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Gauge which stores historic data points with timestamps.
 * This is used for emitting separate data points per request, instead of single aggregated metrics.
 */
public class HistoricGauge implements Gauge<Long> {
  public static class DataPoint {
    Long value;
    long timestamp;

    DataPoint(long value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    public Long getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  private final List<DataPoint> dataPoints = Collections.synchronizedList(new LinkedList<>());

  /**
   * This method will just return first value.
   * @return
   */
  @Override
  public Long getValue() {
    if (!dataPoints.isEmpty()) {
      return dataPoints.get(0).value;
    } else {
      return null;
    }
  }

  public void addDataPoint(Long value) {
    dataPoints.add(new DataPoint(value, System.currentTimeMillis()));
  }

  /**
   * Return copy of dataPoints and remove them from internal list
   * @return copy of the data points
   */
  public List<DataPoint> pollDataPoints() {
    int size = dataPoints.size();
    List<DataPoint> result = new ArrayList<>(dataPoints.subList(0, size));
    if (size > 0) {
      dataPoints.subList(0, size).clear();
    }
    return result;
  }
}
