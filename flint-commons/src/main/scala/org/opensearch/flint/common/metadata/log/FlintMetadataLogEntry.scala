/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.metadata.log

import java.util.{Map => JMap}
import java.util.Base64

import scala.collection.JavaConverters._

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.IndexState

/**
 * Flint metadata log entry. This is temporary and will merge field in FlintMetadata here.
 *
 * @param id
 *   log entry id
 * @param indexName
 *   Flint index name
 * @param dataSource
 *   data source associated //TODO: remove?
 * @param createTime
 *   streaming job start time
 * @param state
 *   Flint index state
 * @param entryVersion
 *   entry version fields for consistency control
 * @param error
 *   error details if in error state
 */
case class FlintMetadataLogEntry(
    id: String,
    indexName: String,
    dataSource: String,
    /**
     * This is currently used as streaming job start time. In future, this should represent the
     * create timestamp of the log entry
     */
    createTime: Long,
    state: IndexState,
    // TODO: consider making EntryVersion class for type check for fields
    entryVersion: Map[String, _],
    error: String) {

  def this(id: String, seqNo: Long, primaryTerm: Long, map: JMap[String, AnyRef]) {
    this(
      id,
      // TODO: avoid decode
      new String(Base64.getDecoder().decode(id)),
      map.get("dataSourceName").asInstanceOf[String],
      /* getSourceAsMap() may use Integer or Long even though it's always long in index mapping */
      map.get("jobStartTime").asInstanceOf[Number].longValue(),
      IndexState.from(map.get("state").asInstanceOf[String]),
      Map("seqNo" -> seqNo, "primaryTerm" -> primaryTerm),
      map.get("error").asInstanceOf[String])
  }

  def this(
      id: String,
      indexName: String,
      dataSource: String,
      createTime: Long,
      state: IndexState,
      entryVersion: JMap[String, _],
      error: String) {
    this(id, indexName, dataSource, createTime, state, entryVersion.asScala.toMap, error)
  }

  // TODO: storage specific. should move to FlintOpenSearchMetadataLog
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

  // TODO: OpenSearch specific stuff should move to FlintOpenSearchMetadataLog
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

  // TODO: OpenSearch specific stuff should move to FlintOpenSearchMetadataLog
  val QUERY_EXECUTION_REQUEST_SETTINGS: String =
    """{
      |  "index": {
      |    "number_of_shards": "1",
      |    "auto_expand_replicas": "0-2",
      |    "number_of_replicas": "0"
      |  }
      |}""".stripMargin
}
