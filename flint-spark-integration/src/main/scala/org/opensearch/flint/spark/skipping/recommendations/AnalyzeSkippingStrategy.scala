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
   * @param inputs
   *   inputs for recommendation strategy. This can table name, columns or functions.
   * @return
   *   skipping index recommendation dataframe
   */
  def analyzeSkippingIndexColumns(
      inputs: Map[String, List[String]],
      spark: SparkSession): Seq[Row]

}
