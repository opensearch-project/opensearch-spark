/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh

import java.util.Collections

import org.opensearch.flint.spark.{FlintSparkIndex, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkException.requireValidation
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
class AutoIndexRefresh(indexName: String, index: FlintSparkIndex) extends FlintSparkIndexRefresh {

  override def refreshMode: RefreshMode = AUTO

  override def validate(spark: SparkSession): Unit = {
    // Incremental refresh cannot enabled at the same time
    val options = index.options
    requireValidation(
      !options.incrementalRefresh(),
      "Incremental refresh cannot be enabled if auto refresh is enabled")

    // Non-Hive table is required for auto refresh
    requireValidation(
      isSourceTableNonHive(spark, index),
      "Index auto refresh doesn't support Hive table")

    // Checkpoint location is required if mandatory option set
    val flintSparkConf = new FlintSparkConf(Collections.emptyMap[String, String])
    val checkpointLocation = index.options.checkpointLocation()
    if (flintSparkConf.isCheckpointMandatory) {
      requireValidation(
        checkpointLocation.isDefined,
        s"Checkpoint location is required if ${CHECKPOINT_MANDATORY.key} option enabled")
    }

    // Given checkpoint location is accessible
    if (checkpointLocation.isDefined) {
      requireValidation(
        isCheckpointLocationAccessible(spark, checkpointLocation.get),
        s"Checkpoint location ${checkpointLocation.get} doesn't exist or no permission to access")
    }
  }

  override def start(spark: SparkSession, flintSparkConf: FlintSparkConf): Option[String] = {
    val options = index.options
    val tableName = index.metadata().source
    index match {
      // Flint index has specialized logic and capability for incremental refresh
      case refresh: StreamingRefresh =>
        logInfo("Start refreshing index in streaming style")
        val job =
          refresh
            .buildStream(spark)
            .writeStream
            .queryName(indexName)
            .format(FLINT_DATASOURCE)
            .options(flintSparkConf.properties)
            .addSinkOptions(options, flintSparkConf)
            .start(indexName)
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
            () // discard return value above and return unit to use right overridden method
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
      dataStream
        .addCheckpointLocation(options.checkpointLocation(), flintSparkConf.isCheckpointMandatory)
        .addRefreshInterval(options.refreshInterval())
        .addAvailableNowTrigger(options.incrementalRefresh())
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

    def addAvailableNowTrigger(incrementalRefresh: Boolean): DataStreamWriter[Row] = {
      if (incrementalRefresh) {
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
