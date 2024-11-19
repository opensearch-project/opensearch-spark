/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import java.util.Collections

import org.opensearch.flint.core.metrics.MetricsSparkListener
import org.opensearch.flint.spark.{FlintSparkIndex, FlintSparkIndexOptions, FlintSparkValidationHelper}
import org.opensearch.flint.spark.FlintSparkIndex.{quotedTableName, StreamingRefresh}
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.{AUTO, RefreshMode}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

/**
 * Index refresh that auto refreshes the index by index options provided.
 *
 * @param indexName
 *   Flint index name
 * @param index
 *   Flint index
 */
class AutoIndexRefresh(indexName: String, index: FlintSparkIndex)
    extends FlintSparkIndexRefresh
    with FlintSparkValidationHelper {

  override def refreshMode: RefreshMode = AUTO

  override def validate(spark: SparkSession): Unit = {
    // Incremental refresh cannot enabled at the same time
    val options = index.options
    require(
      !options.incrementalRefresh(),
      "Incremental refresh cannot be enabled if auto refresh is enabled")

    // Hive table doesn't support auto refresh
    require(
      !isTableProviderSupported(spark, index),
      "Index auto refresh doesn't support Hive table")

    // Checkpoint location is required if mandatory option set or external scheduler is used
    val flintSparkConf = new FlintSparkConf(Collections.emptyMap[String, String])
    val checkpointLocation = options.checkpointLocation()
    if (flintSparkConf.isCheckpointMandatory) {
      require(
        checkpointLocation.isDefined,
        s"Checkpoint location is required if ${CHECKPOINT_MANDATORY.key} option enabled")
    }

    // Checkpoint location must be accessible
    if (checkpointLocation.isDefined) {
      require(
        isCheckpointLocationAccessible(spark, checkpointLocation.get),
        s"No sufficient permission to access the checkpoint location ${checkpointLocation.get}")
    }
  }

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    val options = index.options
    val tableName = index.metadata().source
    index match {
      // Flint index has specialized logic and capability for incremental refresh
      case refresh: StreamingRefresh =>
        logInfo("Start refreshing index in streaming style")
        val job = MetricsSparkListener.withMetrics(
          spark,
          () =>
            refresh
              .buildStream(spark)
              .writeStream
              .queryName(indexName)
              .format(FLINT_DATASOURCE)
              .options(flintSparkConf.properties)
              .addSinkOptions(options, flintSparkConf)
              .start(indexName))
        Some(job.id.toString)

      // Otherwise, fall back to foreachBatch + batch refresh
      case _ =>
        logInfo("Start refreshing index in foreach streaming style")
        val job = spark.readStream
          .options(options.extraSourceOptions(tableName))
          .table(quotedTableName(tableName))
          .writeStream
          .queryName(indexName)
          .addSinkOptions(options, flintSparkConf)
          .foreachBatch { (batchDF: DataFrame, _: Long) =>
            new FullIndexRefresh(indexName, index, Some(batchDF))
              .start(spark, flintSparkConf)
            () // discard return value above and return unit to use the right overridden method
          }
          .start()
        Some(job.id.toString)
    }
  }

  // Using Scala implicit class to avoid breaking method chaining of Spark data frame fluent API
  private implicit class FlintDataStreamWriter(val dataStream: DataStreamWriter[Row]) {

    def addSinkOptions(
        options: FlintSparkIndexOptions,
        flintSparkConf: FlintSparkConf): DataStreamWriter[Row] = {
      // For incremental refresh, the refresh_interval option is overridden by Trigger.AvailableNow().
      dataStream
        .addCheckpointLocation(options.checkpointLocation(), flintSparkConf.isCheckpointMandatory)
        .addRefreshInterval(options.refreshInterval())
        .addAvailableNowTrigger(
          options.isExternalSchedulerEnabled() || options.incrementalRefresh())
        .addOutputMode(options.outputMode())
        .options(options.extraSinkOptions())
    }

    def addCheckpointLocation(
        checkpointLocation: Option[String],
        isCheckpointMandatory: Boolean): DataStreamWriter[Row] = {
      checkpointLocation match {
        case Some(location) => dataStream.option("checkpointLocation", location)
        case None if isCheckpointMandatory =>
          throw new IllegalStateException(
            s"Checkpoint location is mandatory for incremental refresh if ${CHECKPOINT_MANDATORY.key} enabled")
        case _ => dataStream
      }
    }

    def addRefreshInterval(refreshInterval: Option[String]): DataStreamWriter[Row] = {
      refreshInterval
        .map(interval => dataStream.trigger(Trigger.ProcessingTime(interval)))
        .getOrElse(dataStream)
    }

    def addAvailableNowTrigger(setAvailableNow: Boolean): DataStreamWriter[Row] = {
      if (setAvailableNow) {
        dataStream.trigger(Trigger.AvailableNow())
      } else {
        dataStream
      }
    }

    def addOutputMode(outputMode: Option[String]): DataStreamWriter[Row] = {
      outputMode.map(dataStream.outputMode).getOrElse(dataStream)
    }
  }
}
