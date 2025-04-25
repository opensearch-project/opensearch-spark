/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.time.Clock;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Track the current request rate based on the past requests within ESTIMATE_RANGE_DURATION_MSEC
 * milliseconds period.
 */
public class RequestRateMeter {
  private static final long ESTIMATE_RANGE_DURATION_MSEC = 3000;

  private static class DataPoint {
    long timestamp;
    long requestSize;
    public DataPoint(long timestamp, long requestSize) {
      this.timestamp = timestamp;
      this.requestSize = requestSize;
    }
  }

  private final Clock clock;

  private Queue<DataPoint> dataPoints = new LinkedList<>();
  private long currentSum = 0;

  public RequestRateMeter() {
    this(Clock.systemUTC());
  }

  public RequestRateMeter(Clock clock) {
    this.clock = clock;
  }

  public synchronized void addDataPoint(long timestamp, long requestSize) {
    dataPoints.add(new DataPoint(timestamp, requestSize));
    currentSum += requestSize;
    removeOldDataPoints();
  }

  public synchronized long getCurrentEstimatedRate() {
    removeOldDataPoints();
    return currentSum * 1000 / ESTIMATE_RANGE_DURATION_MSEC;
  }

  private synchronized void removeOldDataPoints() {
    long curr = clock.millis();
    while (!dataPoints.isEmpty() && dataPoints.peek().timestamp < curr - ESTIMATE_RANGE_DURATION_MSEC) {
      currentSum -= dataPoints.remove().requestSize;
    }
  }
}
