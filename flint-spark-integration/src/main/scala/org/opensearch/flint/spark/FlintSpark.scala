/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.FlintSparkIndex.{ID_COLUMN, StreamingRefresh}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.{SkippingKind, SkippingKindSerializer}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{DOC_ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN}
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.streaming.Trigger

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(val spark: SparkSession) {

  /** Flint spark configuration */
  private val flintSparkConf: FlintSparkConf =
    FlintSparkConf(
      Map(
        DOC_ID_COLUMN_NAME.optionKey -> ID_COLUMN,
        IGNORE_DOC_ID_COLUMN.optionKey -> "true").asJava)

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient = FlintClientBuilder.build(flintSparkConf.flintOptions())

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def skippingIndex(): FlintSparkSkippingIndex.Builder = {
    new FlintSparkSkippingIndex.Builder(this)
  }

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def coveringIndex(): FlintSparkCoveringIndex.Builder = {
    new FlintSparkCoveringIndex.Builder(this)
  }

  /**
   * Create materialized view builder for creating mv with fluent API.
   *
   * @return
   *   mv builder
   */
  def materializedView(): FlintSparkMaterializedView.Builder = {
    new FlintSparkMaterializedView.Builder(this)
  }

  /**
   * Create the given index with metadata.
   *
   * @param index
   *   Flint index to create
   * @param ignoreIfExists
   *   Ignore existing index
   */
  def createIndex(index: FlintSparkIndex, ignoreIfExists: Boolean = false): Unit = {
    val indexName = index.name()
    if (flintClient.exists(indexName)) {
      if (!ignoreIfExists) {
        throw new IllegalStateException(s"Flint index $indexName already exists")
      }
    } else {
      val metadata = index.metadata()
      flintClient.createIndex(indexName, metadata)
    }
  }

  /**
   * Start refreshing index data according to the given mode.
   *
   * @param indexName
   *   index name
   * @param mode
   *   refresh mode
   * @return
   *   refreshing job ID (empty if batch job for now)
   */
  def refreshIndex(indexName: String, mode: RefreshMode): Option[String] = {
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    val tableName = index.metadata().source

    // Write Flint index data to Flint data source (shared by both refresh modes for now)
    /*
    def writeFlintIndex(df: DataFrame): Unit = {
      index
        .build(df)
        .write
        .format(FLINT_DATASOURCE)
        .options(flintSparkConf.properties)
        .mode(Overwrite)
        .save(indexName)
    }
     */

    def batchRefresh(df: Option[DataFrame] = None): Unit = {
      index
        .build(spark, df)
        .write
        .format(FLINT_DATASOURCE)
        .options(flintSparkConf.properties)
        .mode(Overwrite)
        .save(indexName)
    }

    mode match {
      case FULL if isIncrementalRefreshing(indexName) =>
        throw new IllegalStateException(
          s"Index $indexName is incremental refreshing and cannot be manual refreshed")

      case FULL =>
        batchRefresh()
        None

      case INCREMENTAL if index.isInstanceOf[StreamingRefresh] =>
        val job =
          index
            .asInstanceOf[StreamingRefresh]
            .buildStream(spark)
            .writeStream
            .queryName(indexName)
            .outputMode(Append())
            .format(FLINT_DATASOURCE)
            .options(flintSparkConf.properties)

        index.options
          .checkpointLocation()
          .foreach(location => job.option("checkpointLocation", location))
        index.options
          .refreshInterval()
          .foreach(interval => job.trigger(Trigger.ProcessingTime(interval)))

        val jobId = job.start(indexName).id
        Some(jobId.toString)

      case INCREMENTAL =>
        val job = spark.readStream
          .table(tableName)
          .writeStream
          .queryName(indexName)
          .outputMode(Append())

        index.options
          .checkpointLocation()
          .foreach(location => job.option("checkpointLocation", location))
        index.options
          .refreshInterval()
          .foreach(interval => job.trigger(Trigger.ProcessingTime(interval)))

        val jobId =
          job
            .foreachBatch { (batchDF: DataFrame, _: Long) =>
              batchRefresh(Some(batchDF))
            }
            .start()
            .id
        Some(jobId.toString)
    }
  }

  /**
   * Describe all Flint indexes whose name matches the given pattern.
   *
   * @param indexNamePattern
   *   index name pattern which may contains wildcard
   * @return
   *   Flint index list
   */
  def describeIndexes(indexNamePattern: String): Seq[FlintSparkIndex] = {
    flintClient.getAllIndexMetadata(indexNamePattern).asScala.map(deserialize)
  }

  /**
   * Describe a Flint index.
   *
   * @param indexName
   *   index name
   * @return
   *   Flint index
   */
  def describeIndex(indexName: String): Option[FlintSparkIndex] = {
    if (flintClient.exists(indexName)) {
      val metadata = flintClient.getIndexMetadata(indexName)
      Some(deserialize(metadata))
    } else {
      Option.empty
    }
  }

  /**
   * Delete index and refreshing job associated.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def deleteIndex(indexName: String): Boolean = {
    if (flintClient.exists(indexName)) {
      stopRefreshingJob(indexName)
      flintClient.deleteIndex(indexName)
      true
    } else {
      false
    }
  }

  /**
   * Build data frame for querying the given index. This is mostly for unit test convenience.
   *
   * @param indexName
   *   index name
   * @return
   *   index query data frame
   */
  def queryIndex(indexName: String): DataFrame = {
    spark.read.format(FLINT_DATASOURCE).load(indexName)
  }

  private def isIncrementalRefreshing(indexName: String): Boolean =
    spark.streams.active.exists(_.name == indexName)

  private def stopRefreshingJob(indexName: String): Unit = {
    val job = spark.streams.active.find(_.name == indexName)
    if (job.isDefined) {
      job.get.stop()
    }
  }

  private def deserialize(metadata: FlintMetadata): FlintSparkIndex = {
    val indexOptions = FlintSparkIndexOptions(
      metadata.options.asScala.mapValues(_.asInstanceOf[String]).toMap)

    metadata.kind match {
      case SKIPPING_INDEX_TYPE =>
        val strategies = metadata.indexedColumns.map { colInfo =>
          val skippingKind = SkippingKind.withName(getString(colInfo, "kind"))
          val columnName = getString(colInfo, "columnName")
          val columnType = getString(colInfo, "columnType")

          skippingKind match {
            case PARTITION =>
              PartitionSkippingStrategy(columnName = columnName, columnType = columnType)
            case VALUE_SET =>
              ValueSetSkippingStrategy(columnName = columnName, columnType = columnType)
            case MIN_MAX =>
              MinMaxSkippingStrategy(columnName = columnName, columnType = columnType)
            case other =>
              throw new IllegalStateException(s"Unknown skipping strategy: $other")
          }
        }
        new FlintSparkSkippingIndex(metadata.source, strategies, indexOptions)
      case COVERING_INDEX_TYPE =>
        new FlintSparkCoveringIndex(
          metadata.name,
          metadata.source,
          metadata.indexedColumns.map { colInfo =>
            getString(colInfo, "columnName") -> getString(colInfo, "columnType")
          }.toMap,
          indexOptions)
      case MV_INDEX_TYPE =>
        new FlintSparkMaterializedView(
          metadata.name,
          metadata.source,
          metadata.indexedColumns.map { colInfo =>
            getString(colInfo, "columnName") -> getString(colInfo, "columnType")
          }.toMap,
          indexOptions)
    }
  }

  private def getString(map: java.util.Map[String, AnyRef], key: String): String = {
    map.get(key).asInstanceOf[String]
  }
}

object FlintSpark {

  /**
   * Index refresh mode: FULL: refresh on current source data in batch style at one shot
   * INCREMENTAL: auto refresh on new data in continuous streaming style
   */
  object RefreshMode extends Enumeration {
    type RefreshMode = Value
    val FULL, INCREMENTAL = Value
  }
}
