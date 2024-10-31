/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Collections

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterSkippingStrategy
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Flint Spark index factory that encapsulates specific Flint index instance creation. This is for
 * internal code use instead of user facing API.
 */
object FlintSparkIndexFactory extends Logging {

  /**
   * Creates Flint index from generic Flint metadata.
   *
   * @param spark
   *   Spark session
   * @param metadata
   *   Flint metadata
   * @return
   *   Flint index instance, or None if any error during creation
   */
  def create(spark: SparkSession, metadata: FlintMetadata): Option[FlintSparkIndex] = {
    try {
      Some(doCreate(spark, metadata))
    } catch {
      case e: Exception =>
        logWarning(s"Failed to create Flint index from metadata $metadata", e)
        None
    }
  }

  /**
   * Creates Flint index with default options.
   *
   * @param spark
   *   Spark session
   * @param index
   *   Flint index
   * @return
   *   Flint index with default options
   */
  def createWithDefaultOptions(
      spark: SparkSession,
      index: FlintSparkIndex): Option[FlintSparkIndex] = {
    val originalOptions = index.options
    val updatedOptions =
      FlintSparkIndexOptions.updateOptionsWithDefaults(index.name(), originalOptions)
    val updatedMetadata = index
      .metadata()
      .copy(options = updatedOptions.options.mapValues(_.asInstanceOf[AnyRef]).asJava)
    this.create(spark, updatedMetadata)
  }

  private def doCreate(spark: SparkSession, metadata: FlintMetadata): FlintSparkIndex = {
    val indexOptions = FlintSparkIndexOptions(
      metadata.options.asScala.mapValues(_.asInstanceOf[String]).toMap)
    val latestLogEntry = metadata.latestLogEntry

    // Convert generic Map[String,AnyRef] in metadata to specific data structure in Flint index
    metadata.kind match {
      case SKIPPING_INDEX_TYPE =>
        val strategies = metadata.indexedColumns.map { colInfo =>
          val skippingKind = SkippingKind.withName(getString(colInfo, "kind"))
          val columnName = getString(colInfo, "columnName")
          val columnType = getString(colInfo, "columnType")
          val parameters = getSkipParams(colInfo)

          skippingKind match {
            case PARTITION =>
              PartitionSkippingStrategy(columnName = columnName, columnType = columnType)
            case VALUE_SET =>
              ValueSetSkippingStrategy(
                columnName = columnName,
                columnType = columnType,
                params = parameters)
            case MIN_MAX =>
              MinMaxSkippingStrategy(columnName = columnName, columnType = columnType)
            case BLOOM_FILTER =>
              BloomFilterSkippingStrategy(
                columnName = columnName,
                columnType = columnType,
                params = parameters)
            case other =>
              throw new IllegalStateException(s"Unknown skipping strategy: $other")
          }
        }
        FlintSparkSkippingIndex(metadata.source, strategies, indexOptions, latestLogEntry)
      case COVERING_INDEX_TYPE =>
        FlintSparkCoveringIndex(
          metadata.name,
          metadata.source,
          metadata.indexedColumns.map { colInfo =>
            getString(colInfo, "columnName") -> getString(colInfo, "columnType")
          }.toMap,
          getOptString(metadata.properties, "filterCondition"),
          indexOptions,
          latestLogEntry)
      case MV_INDEX_TYPE =>
        FlintSparkMaterializedView(
          metadata.name,
          metadata.source,
          getMvSourceTables(spark, metadata),
          metadata.indexedColumns.map { colInfo =>
            getString(colInfo, "columnName") -> getString(colInfo, "columnType")
          }.toMap,
          indexOptions,
          latestLogEntry)
    }
  }

  private def getSkipParams(colInfo: java.util.Map[String, AnyRef]): Map[String, String] = {
    colInfo
      .getOrDefault("parameters", Collections.emptyMap())
      .asInstanceOf[java.util.Map[String, String]]
      .asScala
      .toMap
  }

  private def getMvSourceTables(spark: SparkSession, metadata: FlintMetadata): Array[String] = {
    val sourceTables = getArrayString(metadata.properties, "sourceTables")
    if (sourceTables.isEmpty) {
      FlintSparkMaterializedView.extractSourceTableNames(spark, metadata.source)
    } else {
      sourceTables
    }
  }

  private def getString(map: java.util.Map[String, AnyRef], key: String): String = {
    map.get(key).asInstanceOf[String]
  }

  private def getOptString(map: java.util.Map[String, AnyRef], key: String): Option[String] = {
    val value = map.get(key)
    if (value == null) {
      None
    } else {
      Some(value.asInstanceOf[String])
    }
  }

  private def getArrayString(map: java.util.Map[String, AnyRef], key: String): Array[String] = {
    map.get(key) match {
      case list: java.util.ArrayList[_] =>
        list.toArray.map(_.toString)
      case _ => Array.empty[String]
    }
  }
}
