/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.util.LinkedList;
import java.util.List;

/**
 * Track the current request rate based on the past requests within ESTIMATE_RANGE_DURATION_MSEC
 * milliseconds period.
 */
public class RequestRateMeter {
  private static final long ESTIMATE_RANGE_DURATION_MSEC = 3000;

  private static class DataPoint {
    long timestamp;
    long requestCount;
    public DataPoint(long timestamp, long requestCount) {
      this.timestamp = timestamp;
      this.requestCount = requestCount;
    }
  }

  private List<DataPoint> dataPoints = new LinkedList<>();
  private long currentSum = 0;

  synchronized void addDataPoint(long timestamp, long requestCount) {
    dataPoints.add(new DataPoint(timestamp, requestCount));
    currentSum += requestCount;
  }

  synchronized void removeOldDataPoints() {
    long curr = System.currentTimeMillis();
    while (!dataPoints.isEmpty() && dataPoints.get(0).timestamp < curr - ESTIMATE_RANGE_DURATION_MSEC) {
      currentSum -= dataPoints.get(0).requestCount;
      dataPoints.remove(0);
    }
  }

  synchronized long getCurrentEstimatedRate() {
    removeOldDataPoints();
    return currentSum * 1000 / ESTIMATE_RANGE_DURATION_MSEC;
  }
}
