/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.IOException
import java.util.Base64

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.WriteRequest
import org.opensearch.action.update.UpdateRequest
import org.opensearch.client.RequestOptions
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.spark.FlintSparkIndexOperation.FlintMetadataLogEntry

case class FlintSparkIndexOperation[T](flintIndexName: String, flintClient: FlintClient) {

  /** Reuse query request index as Flint metadata log store */
  private val metadataLogIndexName = ".query_request_history_mys3" // + ds name

  /** No need to query Flint index metadata */
  private val latestId = Base64.getEncoder.encodeToString(flintIndexName.getBytes)

  private var beforeCondition: FlintMetadataLogEntry => Boolean = null
  private var transientAction: FlintMetadataLogEntry => FlintMetadataLogEntry = null
  private var opAction: () => T = null
  private var afterAction: FlintMetadataLogEntry => FlintMetadataLogEntry = null

  def before(condition: FlintMetadataLogEntry => Boolean): FlintSparkIndexOperation[T] = {
    this.beforeCondition = condition
    this
  }

  def transient(
      action: FlintMetadataLogEntry => FlintMetadataLogEntry): FlintSparkIndexOperation[T] = {
    this.transientAction = action
    this
  }

  def operation(action: () => T): FlintSparkIndexOperation[T] = {
    this.opAction = action
    this
  }

  def after(
      action: FlintMetadataLogEntry => FlintMetadataLogEntry): FlintSparkIndexOperation[T] = {
    this.afterAction = action
    this
  }

  def execute(): T = {
    var latest = getLatestLogEntry
    if (beforeCondition(latest)) {
      updateDoc(transientAction(latest))
      val result = opAction()
      updateDoc(afterAction(getLatestLogEntry))
      result
    } else {
      throw new IllegalStateException()
    }
  }

  private def getLatestLogEntry: FlintMetadataLogEntry = {
    val latest = getDoc(latestId)
    latest.getOrElse(FlintMetadataLogEntry())
  }

  // Visible for IT
  def getDoc(docId: String): Option[FlintMetadataLogEntry] = {
    val client = flintClient.createClient()
    try {
      val response =
        client.get(new GetRequest(metadataLogIndexName, docId), RequestOptions.DEFAULT)
      Some(
        new FlintMetadataLogEntry(
          response.getId,
          response.getSeqNo,
          response.getPrimaryTerm,
          response.getSourceAsMap))
    } catch {
      case _: Exception => None
    } finally {
      client.close()
    }
  }

  private def createDoc(logEntry: FlintMetadataLogEntry): Unit = {
    val client = flintClient.createClient()
    try {
      client.index(
        new IndexRequest()
          .index(metadataLogIndexName)
          .id(logEntry.docId)
          .source(logEntry.toJson, XContentType.JSON),
        RequestOptions.DEFAULT)
    } finally {
      client.close()
    }
  }

  private def updateDoc(logEntry: FlintMetadataLogEntry): Unit = {
    if (logEntry.docId.isEmpty) {
      createDoc(logEntry.copy(docId = latestId))
    } else {
      updateIf(logEntry.docId, logEntry.toJson, logEntry.seqNo, logEntry.primaryTerm)
    }
  }

  def updateIf(id: String, doc: String, seqNo: Long, primaryTerm: Long): Unit = {
    try {
      val client = flintClient.createClient
      try {
        val updateRequest = new UpdateRequest(metadataLogIndexName, id)
          .doc(doc, XContentType.JSON)
          .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
          .setIfSeqNo(seqNo)
          .setIfPrimaryTerm(primaryTerm)
        client.update(updateRequest, RequestOptions.DEFAULT)
      } catch {
        case e: IOException =>
          throw new RuntimeException(
            String.format(
              "Failed to execute update request on index: %s, id: %s",
              metadataLogIndexName,
              id),
            e)
      } finally if (client != null) client.close()
    }
  }
}

object FlintSparkIndexOperation {

  case class FlintMetadataLogEntry(
      val docId: String = "",
      val seqNo: Long = -1,
      val primaryTerm: Long = -1,
      val state: String = "empty",
      val dataSource: String = "", // TODO: get from Spark conf
      val error: String = "") {

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
         |  "applicationId": "${sys.env.getOrElse(
          "SERVERLESS_EMR_VIRTUAL_CLUSTER_ID",
          "unknown")}",
         |  "jobId": "${sys.env.getOrElse("SERVERLESS_EMR_JOB_ID", "unknown")}",
         |  "dataSourceName": "$dataSource",
         |  "lastUpdateTime": "${System.currentTimeMillis()}",
         |  "error": "$error"
         |}
         |""".stripMargin
    }
  }

  def fromJson(json: String): Unit = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val meta = parse(json)
    val state = (meta \ "state").extract[String]

    val entry = new FlintMetadataLogEntry
    entry.copy(state = state)
  }
}
