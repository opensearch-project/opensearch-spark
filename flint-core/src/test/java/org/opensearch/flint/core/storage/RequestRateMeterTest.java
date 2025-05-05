/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RequestRateMeterTest {

  private RequestRateMeter requestRateMeter;

  @BeforeEach
  void setUp() {
    requestRateMeter = new RequestRateMeter();
  }

  @Test
  void testAddDataPoint() {
    long timestamp = System.currentTimeMillis();
    requestRateMeter.addDataPoint(timestamp, 30);
    assertEquals(3, requestRateMeter.getCurrentEstimatedRate());
  }

  @Test
  void testAddDataPointRemoveOldDataPoint() {
    long timestamp = System.currentTimeMillis();
    requestRateMeter.addDataPoint(timestamp - 11000, 30);
    requestRateMeter.addDataPoint(timestamp, 90);
    assertEquals(90 / 10, requestRateMeter.getCurrentEstimatedRate());
  }

  @Test
  void testRemoveOldDataPoints() {
    long currentTime = System.currentTimeMillis();
    requestRateMeter.addDataPoint(currentTime - 11000, 30);
    requestRateMeter.addDataPoint(currentTime - 5000, 60);
    requestRateMeter.addDataPoint(currentTime, 90);

    assertEquals((60 + 90)/10, requestRateMeter.getCurrentEstimatedRate());
  }

  @Test
  void testGetCurrentEstimatedRate() {
    long currentTime = System.currentTimeMillis();
    requestRateMeter.addDataPoint(currentTime - 2500, 30);
    requestRateMeter.addDataPoint(currentTime - 1500, 60);
    requestRateMeter.addDataPoint(currentTime - 500, 90);

    assertEquals((30 + 60 + 90)/10, requestRateMeter.getCurrentEstimatedRate());
  }

  @Test
  void testEmptyRateMeter() {
    assertEquals(0, requestRateMeter.getCurrentEstimatedRate());
  }

  @Test
  void testSingleDataPoint() {
    requestRateMeter.addDataPoint(System.currentTimeMillis(), 30);
    assertEquals(30 / 10, requestRateMeter.getCurrentEstimatedRate());
  }
}