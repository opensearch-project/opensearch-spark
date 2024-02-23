/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.config

object FlintOptionsMode extends Enumeration {
  type FlintOptionsMode = Value
  val Datasource, Repl = Value
}
