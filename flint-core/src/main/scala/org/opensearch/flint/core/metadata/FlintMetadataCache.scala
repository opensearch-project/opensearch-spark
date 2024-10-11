/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry

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
    val refreshInterval: Option[Int] = {
      // TODO: FlintSparkIndexOptions not available here, hence using literals directly
      if (metadata.options.get("auto_refresh") == "true" && metadata.options.containsKey(
          "refresh_interval")) {
        val tmp = metadata.options.get("refresh_interval").toString
        Some(900) // TODO: parser
      } else {
        None
      }
    }
    val lastRefreshTime: Option[Long] = {
      metadata.latestLogEntry.get.createTime match {
        case FlintMetadataLogEntry.EMPTY_CREATE_TIME => None
        case timestamp => Some(timestamp)
      }
    }
    FlintMetadataCache("1.0", refreshInterval, Array("mock.mock.mock"), lastRefreshTime)
  }
}
