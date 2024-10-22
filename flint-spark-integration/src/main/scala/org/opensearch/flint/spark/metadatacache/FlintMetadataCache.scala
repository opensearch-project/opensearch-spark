/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.spark.FlintSparkIndexOptions

/**
 * Flint metadata cache defines metadata required to store in read cache for frontend user to
 * access.
 */
case class FlintMetadataCache(
    metadataCacheVersion: String,
    /** Refresh interval for Flint index with auto refresh. Unit: seconds */
    refreshInterval: Option[Int],
    /** Source table names for building the Flint index. */
    sourceTables: Array[String],
    /** Timestamp when Flint index is last refreshed. Unit: milliseconds */
    lastRefreshTime: Option[Long]) {

  /**
   * Convert FlintMetadataCache to a map. Skips a field if its value is not defined.
   */
  def toMap: Map[String, AnyRef] = {
    val fieldNames = getClass.getDeclaredFields.map(_.getName)
    val fieldValues = productIterator.toList

    fieldNames
      .zip(fieldValues)
      .flatMap {
        case (_, None) => List.empty
        case (name, Some(value)) => List((name, value))
        case (name, value) => List((name, value))
      }
      .toMap
      .mapValues(_.asInstanceOf[AnyRef])
  }
}

object FlintMetadataCache {

  def apply(metadata: FlintMetadata): FlintMetadataCache = {
    val indexOptions = FlintSparkIndexOptions(
      metadata.options.asScala.mapValues(_.asInstanceOf[String]).toMap)
    val refreshInterval = if (indexOptions.autoRefresh()) {
      indexOptions.refreshInterval().map(_ => 900) // replace with parser function
    } else {
      None
    }
    val lastRefreshTime: Option[Long] = metadata.latestLogEntry.flatMap { entry =>
      entry.lastRefreshCompleteTime match {
        case FlintMetadataLogEntry.EMPTY_TIMESTAMP => None
        case timestamp => Some(timestamp)
      }
    }

    FlintMetadataCache("1.0", refreshInterval, Array("mock.mock.mock"), lastRefreshTime)
  }
}
