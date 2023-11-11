/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

class MockTimeProvider(fixedTime: Long) extends TimeProvider {
  override def currentEpochMillis(): Long = fixedTime
}
