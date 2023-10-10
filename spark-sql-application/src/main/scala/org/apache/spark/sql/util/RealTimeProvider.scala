/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

class RealTimeProvider extends TimeProvider {
  override def currentEpochMillis(): Long = System.currentTimeMillis()
}
