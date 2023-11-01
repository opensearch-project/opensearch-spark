/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log

import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.IndexState

/**
 * Flint metadata log entry. This is temporary and will merge field in FlintMetadata here and move
 * implementation specific field, such as seqNo, primaryTerm, dataSource to properties.
 *
 * @param id
 *   log entry id
 * @param seqNo
 *   OpenSearch sequence number
 * @param primaryTerm
 *   OpenSearch primary term
 * @param state
 *   Flint index state
 * @param dataSource
 *   OpenSearch data source associated //TODO: remove?
 * @param error
 *   error details if in error state
 */
case class FlintMetadataLogEntry(
    id: String,
    seqNo: Long,
    primaryTerm: Long,
    state: IndexState,
    dataSource: String, // TODO: get from Spark conf
    error: String) {

  def this(id: String, seqNo: Long, primaryTerm: Long, map: java.util.Map[String, AnyRef]) {
    this(
      id,
      seqNo,
      primaryTerm,
      IndexState.from(map.get("state").asInstanceOf[String]),
      map.get("dataSourceName").asInstanceOf[String],
      map.get("error").asInstanceOf[String])
  }

  def toJson: String = {
    // Implicitly populate latest appId, jobId and timestamp whenever persist
    s"""
       |{
       |  "version": "1.0",
       |  "type": "flintindexstate",
       |  "state": "$state",
       |  "applicationId": "${sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")}",
       |  "jobId": "${sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown")}",
       |  "dataSourceName": "$dataSource",
       |  "lastUpdateTime": "${System.currentTimeMillis()}",
       |  "error": "$error"
       |}
       |""".stripMargin
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
    val DELETING: IndexState.Value = Value("deleting")
    val DELETED: IndexState.Value = Value("deleted")
    val FAILED: IndexState.Value = Value("failed")
    val RECOVERING: IndexState.Value = Value("recovering")
    val UNKNOWN: IndexState.Value = Value("unknown")

    def from(s: String): IndexState.Value = {
      IndexState.values
        .find(_.toString.equalsIgnoreCase(s))
        .getOrElse(IndexState.UNKNOWN)
    }
  }
}
