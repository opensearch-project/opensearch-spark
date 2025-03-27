/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import org.opensearch.flint.core.metrics.Progress
import org.opensearch.flint.spark.{FlintSparkIndex, FlintSparkValidationHelper}
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.refresh.util.RefreshMetricsAspect

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
class IncrementalIndexRefresh(val indexName: String, index: FlintSparkIndex)
    extends FlintSparkIndexRefresh
    with FlintSparkValidationHelper
    with RefreshMetricsAspect {

  override def refreshMode: RefreshMode = INCREMENTAL

  override def validate(spark: SparkSession): Unit = {
    // Non-Hive table is required for incremental refresh
    require(
      !isTableProviderSupported(spark, index),
      "Index incremental refresh doesn't support Hive table")

    // Checkpoint location is required regardless of mandatory option
    val options = index.options
    val checkpointLocation = options.checkpointLocation()
    require(options.checkpointLocation().nonEmpty, "Checkpoint location is required")
    require(
      isCheckpointLocationAccessible(spark, checkpointLocation.get),
      s"No sufficient permission to access the checkpoint location ${checkpointLocation.get}")
  }

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    logInfo(s"Start refreshing index $indexName in incremental mode")

    val clientId = flintSparkConf.flintOptions().getAWSAccountId()
    val dataSource = flintSparkConf.flintOptions().getDataSourceName()

    withMetrics(clientId, dataSource, indexName, "incrementalRefresh") {
      // Reuse auto refresh which uses AvailableNow trigger and will stop once complete
      val jobId =
        new AutoIndexRefresh(indexName, index)
          .start(spark, flintSparkConf)

      // Blocks the calling thread until the streaming query finishes
      spark.streams
        .get(jobId.get)
        .awaitTermination()

      None
    }
  }

  override def progress(): Option[Progress] = None
}
