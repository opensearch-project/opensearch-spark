/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.apache.spark.sql.util.TimeProvider

class MockTimeProvider(fixedTime: Long) extends TimeProvider {
  override def currentEpochMillis(): Long = fixedTime
}
