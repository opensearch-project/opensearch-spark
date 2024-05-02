/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.skipping.recommendations

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Automate skipping index column and algorithm selection interface.
 */
trait AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param data
   *   data for recommendation strategy.
   * @return
   *   skipping index recommendation dataframe.
   */
  def analyzeSkippingIndexColumns(data: DataFrame, spark: SparkSession): Seq[Row]

}
