/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, JArray, NoTypeHints}
import org.json4s.JsonAST.{JField, JObject}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL, RefreshMode}
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
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
      flintClient.createIndex(indexName, index.metadata())
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
    val tableName = getSourceTableName(index)

    // Write Flint index data to Flint data source (shared by both refresh modes for now)
    def writeFlintIndex(df: DataFrame): Unit = {
      index
        .build(df)
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
        writeFlintIndex(
          spark.read
            .table(tableName))
        None

      case INCREMENTAL =>
        // TODO: Use Foreach sink for now. Need to move this to FlintSparkSkippingIndex
        //  once finalized. Otherwise, covering index/MV may have different logic.
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
              writeFlintIndex(batchDF)
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

  // TODO: Remove all parsing logic below once Flint spec finalized and FlintMetadata strong typed
  private def getSourceTableName(index: FlintSparkIndex): String = {
    val json = parse(index.metadata().getContent)
    (json \ "_meta" \ "source").extract[String]
  }

  /*
   * For now, deserialize skipping strategies out of Flint metadata json
   * ex. extract Seq(Partition("year", "int"), ValueList("name")) from
   *  { "_meta": { "indexedColumns": [ {...partition...}, {...value list...} ] } }
   *
   */
  private def deserialize(metadata: FlintMetadata): FlintSparkIndex = {
    val meta = parse(metadata.getContent) \ "_meta"
    val indexName = (meta \ "name").extract[String]
    val tableName = (meta \ "source").extract[String]
    val indexType = (meta \ "kind").extract[String]
    val indexedColumns = (meta \ "indexedColumns").asInstanceOf[JArray]
    val indexOptions = FlintSparkIndexOptions(
      (meta \ "options")
        .asInstanceOf[JObject]
        .obj
        .map { case JField(key, value) =>
          key -> value.values.toString
        }
        .toMap)

    indexType match {
      case SKIPPING_INDEX_TYPE =>
        val strategies = indexedColumns.arr.map { colInfo =>
          val skippingKind = SkippingKind.withName((colInfo \ "kind").extract[String])
          val columnName = (colInfo \ "columnName").extract[String]
          val columnType = (colInfo \ "columnType").extract[String]

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
        new FlintSparkSkippingIndex(tableName, strategies, indexOptions)
      case COVERING_INDEX_TYPE =>
        new FlintSparkCoveringIndex(
          indexName,
          tableName,
          indexedColumns.arr.map { obj =>
            ((obj \ "columnName").extract[String], (obj \ "columnType").extract[String])
          }.toMap,
          indexOptions)
    }
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
