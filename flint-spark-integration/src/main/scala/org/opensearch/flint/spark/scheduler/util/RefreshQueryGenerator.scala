/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler.util

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex

object RefreshQueryGenerator {

  /**
   * Generates a scheduled query string for refreshing a Flint index.
   *
   * @param index
   *   Flint index
   * @return
   *   the generated scheduled query string
   * @throws IllegalArgumentException
   *   if the provided parameters are invalid
   */
  def generateRefreshQuery(index: FlintSparkIndex): String = {
    index match {
      case skippingIndex: FlintSparkSkippingIndex =>
        s"REFRESH SKIPPING INDEX ON ${skippingIndex.tableName}"
      case coveringIndex: FlintSparkCoveringIndex =>
        s"REFRESH INDEX ${coveringIndex.indexName} ON ${coveringIndex.tableName}"
      case materializedView: FlintSparkMaterializedView =>
        s"REFRESH MATERIALIZED VIEW ${materializedView.mvName}"
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported index type: ${index.getClass.getSimpleName}")
    }
  }
}
