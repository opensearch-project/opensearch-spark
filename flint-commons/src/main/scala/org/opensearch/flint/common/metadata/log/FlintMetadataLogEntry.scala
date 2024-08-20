/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata.log

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.IndexState

/**
 * Flint metadata log entry. This is temporary and will merge field in FlintMetadata here.
 *
 * @param id
 *   log entry id
 * @param state
 *   Flint index state
 * @param entryVersion
 *   entry version fields for consistency control
 * @param error
 *   error details if in error state
 * @param properties
 *   extra properties fields
 */
case class FlintMetadataLogEntry(
    id: String,
    /**
     * This is currently used as streaming job start time. In future, this should represent the
     * create timestamp of the log entry
     */
    createTime: Long,
    state: IndexState,
    entryVersion: Map[String, Any],
    error: String,
    properties: Map[String, Any]) {

  def this(
      id: String,
      createTime: Long,
      state: IndexState,
      entryVersion: JMap[String, Any],
      error: String,
      properties: JMap[String, Any]) = {
    this(id, createTime, state, entryVersion.asScala.toMap, error, properties.asScala.toMap)
  }

  def this(
      id: String,
      createTime: Long,
      state: IndexState,
      entryVersion: JMap[String, Any],
      error: String,
      properties: Map[String, Any]) = {
    this(id, createTime, state, entryVersion.asScala.toMap, error, properties)
  }
}

object FlintMetadataLogEntry {

  /**
   * Flint index state enum.
   */
  object IndexState extends Enumeration {
    type IndexState = Value
    val EMPTY: IndexState.Value = Value("empty")
    val CREATING: IndexState.Value = Value("creating")
    val ACTIVE: IndexState.Value = Value("active")
    val REFRESHING: IndexState.Value = Value("refreshing")
    val UPDATING: IndexState.Value = Value("updating")
    val DELETING: IndexState.Value = Value("deleting")
    val DELETED: IndexState.Value = Value("deleted")
    val FAILED: IndexState.Value = Value("failed")
    val RECOVERING: IndexState.Value = Value("recovering")
    val VACUUMING: IndexState.Value = Value("vacuuming")
    val UNKNOWN: IndexState.Value = Value("unknown")

    def from(s: String): IndexState.Value = {
      IndexState.values
        .find(_.toString.equalsIgnoreCase(s))
        .getOrElse(IndexState.UNKNOWN)
    }
  }
}
