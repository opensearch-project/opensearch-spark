/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log

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
    id: String = "",
    seqNo: Long = -1,
    primaryTerm: Long = -1,
    state: String = "empty",
    dataSource: String = "", // TODO: get from Spark conf
    error: String = "") {

  def this(docId: String, seqNo: Long, primaryTerm: Long, map: java.util.Map[String, AnyRef]) {
    this(
      docId,
      seqNo,
      primaryTerm,
      map.get("state").asInstanceOf[String],
      map.get("dataSourceName").asInstanceOf[String],
      map.get("error").asInstanceOf[String])
  }

  def toJson: String = {
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
