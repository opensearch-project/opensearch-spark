/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{INCREMENTAL, RefreshMode}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Index refresh that incrementally refreshes the index from the last checkpoint.
 *
 * @param indexName
 *   Flint index name
 * @param index
 *   Flint index
 */
class IncrementalIndexRefresh(indexName: String, index: FlintSparkIndex)
    extends FlintSparkIndexRefresh {

  override def refreshMode: RefreshMode = INCREMENTAL

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    logInfo(s"Start refreshing index $indexName in incremental mode")

    // Reuse auto refresh which uses AvailableNow trigger and will stop once complete
    val jobId =
      new AutoIndexRefresh(indexName, index)
        .start(spark, flintSparkConf)

    spark.streams
      .get(jobId.get)
      .awaitTermination()
    None
  }
}
