/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.{Failure, Success, Try}

import org.json4s.native.Serialization
import org.opensearch.flint.common.model.InteractiveSession
import org.opensearch.flint.common.model.InteractiveSession.formats
import org.opensearch.flint.core.logging.CustomLogging

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SessionUpdateMode.SessionUpdateMode
import org.apache.spark.sql.flint.config.FlintSparkConf

class SessionManagerImpl(spark: SparkSession, resultIndexOptionIn: Option[String])
    extends SessionManager
    with FlintJobExecutor
    with Logging {

  var resultIndexOption = resultIndexOptionIn
  if (resultIndexOption.isEmpty) {
//    logAndThrow("resultIndex is not set")
    resultIndexOption = Some("stub_index")
  }

  // we don't allow default value for sessionIndex. Throw exception if key not found.
  val sessionIndex: String = spark.conf.get(FlintSparkConf.REQUEST_INDEX.key, "")

  if (sessionIndex.isEmpty) {
    logAndThrow(FlintSparkConf.REQUEST_INDEX.key + " is not set")
  }

  val osClient = new OSClient(FlintSparkConf().flintOptions())
  lazy val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex)

  override def getSessionContext: Map[String, Any] = {
    Map(
      "sessionIndex" -> sessionIndex,
      "resultIndex" -> resultIndexOption.get,
      "osClient" -> osClient,
      "flintSessionIndexUpdater" -> flintSessionIndexUpdater)
  }

  override def getSessionDetails(sessionId: String): Option[InteractiveSession] = {
    Try(osClient.getDoc(sessionIndex, sessionId)) match {
      case Success(getResponse) if getResponse.isExists =>
        // Retrieve the source map and create session
        val sessionOption = Option(getResponse.getSourceAsMap)
          .map(InteractiveSession.deserializeFromMap)
        // Retrieve sequence number and primary term from the response
        val seqNo = getResponse.getSeqNo
        val primaryTerm = getResponse.getPrimaryTerm

        // Add seqNo and primaryTerm to the session context
        sessionOption.foreach { session =>
          session.setContextValue("seqNo", seqNo)
          session.setContextValue("primaryTerm", primaryTerm)
        }

        sessionOption
      case Failure(exception) =>
        CustomLogging.logError(
          s"Failed to retrieve existing InteractiveSession: ${exception.getMessage}",
          exception)
        None

      case _ => None
    }
  }

  override def updateSessionDetails(
      sessionDetails: InteractiveSession,
      sessionUpdateMode: SessionUpdateMode): Unit = {
    sessionUpdateMode match {
      case SessionUpdateMode.UPDATE =>
        flintSessionIndexUpdater.update(
          sessionDetails.sessionId,
          InteractiveSession.serialize(sessionDetails, currentTimeProvider.currentEpochMillis()))
      case SessionUpdateMode.UPSERT =>
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
      case SessionUpdateMode.UPDATE_IF =>
        val seqNo = sessionDetails
          .getContextValue("seqNo")
          .getOrElse(throw new IllegalArgumentException("Missing seqNo for conditional update"))
          .asInstanceOf[Long]
        val primaryTerm = sessionDetails
          .getContextValue("primaryTerm")
          .getOrElse(
            throw new IllegalArgumentException("Missing primaryTerm for conditional update"))
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

  override def recordHeartbeat(sessionId: String): Unit = {
    flintSessionIndexUpdater.upsert(
      sessionId,
      Serialization.write(
        Map("lastUpdateTime" -> currentTimeProvider.currentEpochMillis(), "state" -> "running")))
  }
}
