/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.{Failure, Success, Try}

import org.json4s.native.Serialization
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.data.FlintInstance
import org.opensearch.flint.data.FlintInstance.formats

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.REQUEST_INDEX

// Default SessionManager which takes OS client
class SessionManagerImpl(flintSparkConf: FlintSparkConf)
    extends SessionManager
    with FlintJobExecutor
    with Logging {
  val flintOptions: FlintOptions = flintSparkConf.flintOptions()

  // Session Index is required for OS client
  val sessionIndex: Option[String] = Option(flintOptions.getRequestMetadata)

  if (sessionIndex.isEmpty) {
    logAndThrow(FlintSparkConf.REQUEST_INDEX.key + " is not set")
  }

  val osClient = new OSClient(flintOptions)
  val flintSessionIndexUpdater = osClient.createUpdater(sessionIndex.get)

  override def getSessionDetails(sessionId: String): Option[FlintInstance] = {
    Try(osClient.getDoc(sessionIndex.get, sessionId)) match {
      case Success(getResponse) if getResponse.isExists =>
        Option(getResponse.getSourceAsMap)
          .map(FlintInstance.deserializeFromMap)
      case Failure(exception) =>
        CustomLogging.logError(
          s"Failed to retrieve existing FlintInstance: ${exception.getMessage}",
          exception)
        None
      case _ => None
    }
  }

  override def updateSessionDetails(
      sessionDetails: FlintInstance,
      updateMode: UpdateMode.Value): Unit = {

    updateMode match {
      case UpdateMode.Update =>
        flintSessionIndexUpdater.update(
          sessionDetails.sessionId,
          FlintInstance.serialize(sessionDetails, currentTimeProvider.currentEpochMillis()))
      case UpdateMode.Upsert =>
        val includeJobId =
          !sessionDetails.excludedJobIds.isEmpty && !sessionDetails.excludedJobIds.contains(
            sessionDetails.jobId)
        val serializedSession = if (includeJobId) {
          FlintInstance.serialize(sessionDetails, currentTimeProvider.currentEpochMillis(), true)
        } else {
          FlintInstance.serializeWithoutJobId(
            sessionDetails,
            currentTimeProvider.currentEpochMillis())
        }
        flintSessionIndexUpdater.upsert(sessionDetails.sessionId, serializedSession)
      case UpdateMode.UpdateIf =>
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
          FlintInstance.serializeWithoutJobId(
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

  override def exclusionCheck(sessionId: String, excludeJobIds: List[String]): Boolean = {
    val getResponse = osClient.getDoc(sessionIndex.get, sessionId)
    if (getResponse.isExists()) {
      val source = getResponse.getSourceAsMap
      if (source != null) {
        val existingExcludedJobIds = parseExcludedJobIds(source)
        if (excludeJobIds.sorted == existingExcludedJobIds.sorted) {
          logInfo("duplicate job running, exit the application.")
          return true
        }
      }
    }
    false
  }

  /**
   * Reads the session store to get excluded jobs and the current job ID. If the current job ID
   * (myJobId) is not the running job ID (runJobId), or if myJobId is in the list of excluded
   * jobs, it returns false. The first condition ensures we only have one active job for one
   * session thus avoid race conditions on statement execution and states. The 2nd condition
   * ensures we don't pick up a job that has been excluded from the session store and thus CP has
   * a way to notify spark when deployments are happening. If excludeJobs is null or none of the
   * above conditions are met, it returns true.
   *
   * @return
   *   whether we can start fetching next statement or not
   */
  override def canPickNextCommand(sessionId: String, jobId: String): Boolean = {
    try {
      val getResponse = osClient.getDoc(sessionIndex.get, sessionId)
      if (getResponse.isExists()) {
        val source = getResponse.getSourceAsMap
        if (source == null) {
          logError(s"""Session id ${sessionId} is empty""")
          // still proceed since we are not sure what happened (e.g., OpenSearch cluster may be unresponsive)
          return true
        }

        val runJobId = Option(source.get("jobId")).map(_.asInstanceOf[String]).orNull
        val excludeJobIds: Seq[String] = parseExcludedJobIds(source)

        if (runJobId != null && jobId != runJobId) {
          logInfo(s"""the current job ID ${jobId} is not the running job ID ${runJobId}""")
          return false
        }
        if (excludeJobIds != null && excludeJobIds.contains(jobId)) {
          logInfo(s"""${jobId} is in the list of excluded jobs""")
          return false
        }
        true
      } else {
        // still proceed since we are not sure what happened (e.g., session doc may not be available yet)
        logError(s"""Fail to find id ${sessionId} from session index""")
        true
      }
    } catch {
      // still proceed since we are not sure what happened (e.g., OpenSearch cluster may be unresponsive)
      case e: Exception =>
        CustomLogging.logError(s"""Fail to find id ${sessionId} from session index.""", e)
        true
    }
  }
  override def getSessionManagerMetadata: Map[String, Any] = {
    Map(
      "sessionIndex" -> sessionIndex,
      "osClient" -> osClient,
      "flintSessionIndexUpdater" -> flintSessionIndexUpdater)
  }
  private def parseExcludedJobIds(source: java.util.Map[String, AnyRef]): Seq[String] = {
    val rawExcludeJobIds = source.get("excludeJobIds")
    Option(rawExcludeJobIds)
      .map {
        case s: String => Seq(s)
        case list: java.util.List[_] @unchecked =>
          import scala.collection.JavaConverters._
          list.asScala.toList
            .collect { case str: String => str } // Collect only strings from the list
        case other =>
          logInfo(s"Unexpected type: ${other.getClass.getName}")
          Seq.empty
      }
      .getOrElse(Seq.empty[String]) // In case of null, return an empty Seq
  }
}
