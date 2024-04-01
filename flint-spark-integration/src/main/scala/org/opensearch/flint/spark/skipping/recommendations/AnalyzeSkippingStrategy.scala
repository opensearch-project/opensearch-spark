/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.Map

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Automate skipping index column and algorithm selection.
 */
trait AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param tableName
   *   table name
   * @return
   *   skipping index recommendation dataframe
   */
  def analyzeSkippingIndexColumns(
      tableName: String,
      columnsMap: Map[String, Set[String]] = null,
      spark: SparkSession): Seq[Row]

}
