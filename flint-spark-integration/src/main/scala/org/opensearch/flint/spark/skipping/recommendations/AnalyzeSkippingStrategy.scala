/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.skipping.recommendations

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Automate skipping index column and algorithm selection interface.
 */
trait AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param tableName
   *   table name
   * @param columns
   *   list of columns
   * @return
   *   skipping index recommendation dataframe
   */
  def analyzeSkippingIndexColumns(
      tableName: String,
      columns: List[String] = null,
      spark: SparkSession): Seq[Row]

}
