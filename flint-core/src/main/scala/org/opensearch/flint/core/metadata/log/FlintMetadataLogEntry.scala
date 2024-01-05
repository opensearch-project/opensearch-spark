/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata.log

import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.IndexState
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}

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
    /**
     * This is currently used as streaming job start time. In future, this should represent the
     * create timestamp of the log entry
     */
    createTime: Long,
    state: IndexState,
    dataSource: String,
    error: String) {

  def this(id: String, seqNo: Long, primaryTerm: Long, map: java.util.Map[String, AnyRef]) {
    this(
      id,
      seqNo,
      primaryTerm,
      /* getSourceAsMap() may use Integer or Long even though it's always long in index mapping */
      map.get("jobStartTime").asInstanceOf[Number].longValue(),
      IndexState.from(map.get("state").asInstanceOf[String]),
      map.get("dataSourceName").asInstanceOf[String],
      map.get("error").asInstanceOf[String])
  }

  def toJson: String = {
    // Implicitly populate latest appId, jobId and timestamp whenever persist
    s"""
       |{
       |  "version": "1.0",
       |  "latestId": "$id",
       |  "type": "flintindexstate",
       |  "state": "$state",
       |  "applicationId": "${sys.env.getOrElse("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown")}",
       |  "jobId": "${sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown")}",
       |  "dataSourceName": "$dataSource",
       |  "jobStartTime": $createTime,
       |  "lastUpdateTime": ${System.currentTimeMillis()},
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
    val VACUUMING: IndexState.Value = Value("vacuuming")
    val UNKNOWN: IndexState.Value = Value("unknown")

    def from(s: String): IndexState.Value = {
      IndexState.values
        .find(_.toString.equalsIgnoreCase(s))
        .getOrElse(IndexState.UNKNOWN)
    }
  }

  val QUERY_EXECUTION_REQUEST_MAPPING: String =
    """{
      |  "dynamic": false,
      |  "properties": {
      |    "version": {
      |      "type": "keyword"
      |    },
      |    "type": {
      |      "type": "keyword"
      |    },
      |    "state": {
      |      "type": "keyword"
      |    },
      |    "statementId": {
      |      "type": "keyword"
      |    },
      |    "applicationId": {
      |      "type": "keyword"
      |    },
      |    "sessionId": {
      |      "type": "keyword"
      |    },
      |    "sessionType": {
      |      "type": "keyword"
      |    },
      |    "error": {
      |      "type": "text"
      |    },
      |    "lang": {
      |      "type": "keyword"
      |    },
      |    "query": {
      |      "type": "text"
      |    },
      |    "dataSourceName": {
      |      "type": "keyword"
      |    },
      |    "submitTime": {
      |      "type": "date",
      |      "format": "strict_date_time||epoch_millis"
      |    },
      |    "jobId": {
      |      "type": "keyword"
      |    },
      |    "lastUpdateTime": {
      |      "type": "date",
      |      "format": "strict_date_time||epoch_millis"
      |    },
      |    "queryId": {
      |      "type": "keyword"
      |    },
      |    "excludeJobIds": {
      |      "type": "keyword"
      |    }
      |  }
      |}""".stripMargin

  val QUERY_EXECUTION_REQUEST_SETTINGS: String =
    """{
      |  "index": {
      |    "number_of_shards": "1",
      |    "auto_expand_replicas": "0-2",
      |    "number_of_replicas": "0"
      |  }
      |}""".stripMargin

  def failLogEntry(dataSourceName: String, error: String): FlintMetadataLogEntry =
    FlintMetadataLogEntry(
      "",
      UNASSIGNED_SEQ_NO,
      UNASSIGNED_PRIMARY_TERM,
      0L,
      IndexState.FAILED,
      dataSourceName,
      error)
}
