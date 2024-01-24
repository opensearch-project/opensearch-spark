/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresher.RefreshMode.{INCREMENTAL, RefreshMode}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Index refresher that manually and incrementally refreshes the index from last checkpoint.
 *
 * @param indexName
 *   Flint index name
 * @param index
 *   Flint index
 */
class IncrementalIndexRefresher(indexName: String, index: FlintSparkIndex)
    extends FlintSparkIndexRefresher {

  override def refreshMode: RefreshMode = INCREMENTAL

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    logInfo(s"Start refreshing index $indexName in incremental mode")
    val jobId =
      new AutoIndexRefresher(indexName, index)
        .start(spark, flintSparkConf)

    // Streaming job will stop because AvailableNow trigger is in use if incremental
    spark.streams.get(jobId.get).awaitTermination()
    None
  }
}
