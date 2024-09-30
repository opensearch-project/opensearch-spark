/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata

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
  // TODO: construct FlintMetadataCache from FlintMetadata

  def mock: FlintMetadataCache = {
    // Fixed dummy data
    FlintMetadataCache(
      "1.0",
      Some(900),
      Array(
        "dataSourceName.default.logGroups(logGroupIdentifier:['arn:aws:logs:us-east-1:123456:test-llt-xa', 'arn:aws:logs:us-east-1:123456:sample-lg-1'])"),
      Some(1727395328283L))
  }
}
