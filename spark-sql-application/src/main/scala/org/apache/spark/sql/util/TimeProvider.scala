/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

/**
 * A trait to provide current epoch time in milliseconds. This trait helps make it current time
 * provider mockable.
 */
trait TimeProvider {
  def currentEpochMillis(): Long
}
