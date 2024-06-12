/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.{Failure, Success, Try}

import org.json4s.native.Serialization
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.storage.FlintReader
import org.opensearch.flint.data.InteractiveSession
import org.opensearch.flint.data.InteractiveSession.formats
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SessionUpdateMode.SessionUpdateMode
import org.apache.spark.sql.flint.config.FlintSparkConf

class SessionManagerImpl(flintOptions: FlintOptions)
    extends SessionManager
    with FlintJobExecutor
    with Logging {

  // we don't allow default value for sessionIndex, sessionId and datasource. Throw exception if key not found.
  val sessionIndex: String = flintOptions.getSystemIndexName
  val sessionId: String = flintOptions.getSessionId
  val dataSource: String = flintOptions.getDataSourceName

  if (sessionIndex.isEmpty) {
    logAndThrow(FlintSparkConf.REQUEST_INDEX.key + " is not set")
  }
  if (sessionId.isEmpty) {
    logAndThrow(FlintSparkConf.SESSION_ID.key + " is not set")
  }
  if (dataSource.isEmpty) {
    logAndThrow(FlintSparkConf.DATA_SOURCE_NAME.key + " is not set")
  }

  val osClient = new OSClient(flintOptions)
  val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex)
  val flintReader: FlintReader = createQueryReader(sessionId, sessionIndex, dataSource)

  override def getSessionManagerMetadata: Map[String, Any] = {
    Map(
      "sessionIndex" -> sessionIndex,
      "osClient" -> osClient,
      "flintSessionIndexUpdater" -> flintSessionIndexUpdater,
      "flintReader" -> flintReader)
  }

  override def getSessionDetails(sessionId: String): Option[InteractiveSession] = {
    Try(osClient.getDoc(sessionIndex, sessionId)) match {
      case Success(getResponse) if getResponse.isExists =>
        Option(getResponse.getSourceAsMap)
          .map(InteractiveSession.deserializeFromMap)
      case Failure(exception) =>
        CustomLogging.logError(
          s"Failed to retrieve existing InteractiveSession: ${exception.getMessage}",
          exception)
        None
      case _ => None
    }
  }

  override def recordHeartbeat(sessionId: String): Unit = {
    flintSessionIndexUpdater.upsert(
      sessionId,
      Serialization.write(
        Map("lastUpdateTime" -> currentTimeProvider.currentEpochMillis(), "state" -> "running")))
  }

  override def hasPendingStatement(sessionId: String): Boolean = {
    flintReader.hasNext
  }

  private def createQueryReader(sessionId: String, sessionIndex: String, dataSource: String) = {
    // all state in index are in lower case
    // we only search for statement submitted in the last hour in case of unexpected bugs causing infinite loop in the
    // same doc
    val dsl =
      s"""{
       |  "bool": {
       |    "must": [
       |    {
       |        "term": {
       |          "type": "statement"
       |        }
       |      },
       |      {
       |        "term": {
       |          "state": "waiting"
       |        }
       |      },
       |      {
       |        "term": {
       |          "sessionId": "$sessionId"
       |        }
       |      },
       |      {
       |        "term": {
       |          "dataSourceName": "$dataSource"
       |        }
       |      },
       |      {
       |        "range": {
       |          "submitTime": { "gte": "now-1h" }
       |        }
       |      }
       |    ]
       |  }
       |}""".stripMargin

    val flintReader = osClient.createQueryReader(sessionIndex, dsl, "submitTime", SortOrder.ASC)
    flintReader
  }

  override def updateSessionDetails(
      sessionDetails: InteractiveSession,
      sessionUpdateMode: SessionUpdateMode): Unit = {
    sessionUpdateMode match {
      case SessionUpdateMode.Update =>
        flintSessionIndexUpdater.update(
          sessionDetails.sessionId,
          InteractiveSession.serialize(sessionDetails, currentTimeProvider.currentEpochMillis()))
      case SessionUpdateMode.Upsert =>
        val includeJobId =
          !sessionDetails.excludedJobIds.isEmpty && !sessionDetails.excludedJobIds.contains(
            sessionDetails.jobId)
        val serializedSession = if (includeJobId) {
          InteractiveSession.serialize(
            sessionDetails,
            currentTimeProvider.currentEpochMillis(),
            true)
        } else {
          InteractiveSession.serializeWithoutJobId(
            sessionDetails,
            currentTimeProvider.currentEpochMillis())
        }
        flintSessionIndexUpdater.upsert(sessionDetails.sessionId, serializedSession)
      case SessionUpdateMode.UpdateIf =>
        val executionContext = sessionDetails.executionContext.getOrElse(
          throw new IllegalArgumentException("Missing executionContext for conditional update"))
        val seqNo = executionContext
          .get("_seq_no")
          .getOrElse(throw new IllegalArgumentException("Missing _seq_no for conditional update"))
          .asInstanceOf[Long]
        val primaryTerm = executionContext
          .get("_primary_term")
          .getOrElse(
            throw new IllegalArgumentException("Missing _primary_term for conditional update"))
          .asInstanceOf[Long]
        flintSessionIndexUpdater.updateIf(
          sessionDetails.sessionId,
          InteractiveSession.serializeWithoutJobId(
            sessionDetails,
            currentTimeProvider.currentEpochMillis()),
          seqNo,
          primaryTerm)
    }

    logInfo(
      s"""Updated job: {"jobid": ${sessionDetails.jobId}, "sessionId": ${sessionDetails.sessionId}} from $sessionIndex""")
  }
}
