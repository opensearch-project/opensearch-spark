/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.scheduler.util.IntervalSchedulerParser

import org.apache.spark.internal.Logging

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

object FlintMetadataCache extends Logging {

  val metadataCacheVersion = "1.0"

  def apply(metadata: FlintMetadata): FlintMetadataCache = {
    val indexOptions = FlintSparkIndexOptions(
      metadata.options.asScala.mapValues(_.asInstanceOf[String]).toMap)
    val refreshInterval = if (indexOptions.autoRefresh()) {
      indexOptions
        .refreshInterval()
        .map(IntervalSchedulerParser.parseAndConvertToMillis)
        .map(millis => (millis / 1000).toInt) // convert to seconds
    } else {
      None
    }
    val sourceTables = metadata.kind match {
      case MV_INDEX_TYPE =>
        val sourceTables = metadata.properties.get("sourceTables")
        logInfo(s"sourceTables: $sourceTables")
        sourceTables match {
          case list: java.util.ArrayList[_] =>
            logInfo("sourceTables is ArrayList")
            list.toArray.map(_.toString)
          case _ =>
            logInfo(s"sourceTables is not ArrayList. It is of type: ${sourceTables.getClass.getName}")
            Array.empty[String]
        }
      case _ => Array(metadata.source)
    }
    val lastRefreshTime: Option[Long] = metadata.latestLogEntry.flatMap { entry =>
      entry.lastRefreshCompleteTime match {
        case FlintMetadataLogEntry.EMPTY_TIMESTAMP => None
        case timestamp => Some(timestamp)
      }
    }

    FlintMetadataCache(metadataCacheVersion, refreshInterval, sourceTables, lastRefreshTime)
  }
}
