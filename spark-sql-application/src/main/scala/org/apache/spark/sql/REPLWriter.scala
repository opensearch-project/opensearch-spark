/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

trait REPLWriter {
  def write(dataFrame: DataFrame, destination: String): Unit
}
