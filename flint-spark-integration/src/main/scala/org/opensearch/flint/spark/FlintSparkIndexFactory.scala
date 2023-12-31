/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{MIN_MAX, PARTITION, VALUE_SET}
import org.opensearch.flint.spark.skipping.minmax.MinMaxSkippingStrategy
import org.opensearch.flint.spark.skipping.partition.PartitionSkippingStrategy
import org.opensearch.flint.spark.skipping.valueset.ValueSetSkippingStrategy

/**
 * Flint Spark index factory that encapsulates specific Flint index instance creation. This is for
 * internal code use instead of user facing API.
 */
object FlintSparkIndexFactory {

  /**
   * Creates Flint index from generic Flint metadata.
   *
   * @param metadata
   *   Flint metadata
   * @return
   *   Flint index
   */
  def create(metadata: FlintMetadata): FlintSparkIndex = {
    val indexOptions = FlintSparkIndexOptions(
      metadata.options.asScala.mapValues(_.asInstanceOf[String]).toMap)

    // Convert generic Map[String,AnyRef] in metadata to specific data structure in Flint index
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
        FlintSparkSkippingIndex(metadata.source, strategies, indexOptions)
      case COVERING_INDEX_TYPE =>
        FlintSparkCoveringIndex(
          metadata.name,
          metadata.source,
          metadata.indexedColumns.map { colInfo =>
            getString(colInfo, "columnName") -> getString(colInfo, "columnType")
          }.toMap,
          getOptString(metadata.properties, "filterCondition"),
          indexOptions)
      case MV_INDEX_TYPE =>
        FlintSparkMaterializedView(
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

  private def getOptString(map: java.util.Map[String, AnyRef], key: String): Option[String] = {
    val value = map.get(key)
    if (value == null) {
      None
    } else {
      Some(value.asInstanceOf[String])
    }
  }
}
