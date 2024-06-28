/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.data

import java.util.{List => JavaList, Map => JavaMap}

import scala.collection.JavaConverters._

import org.json4s.{Formats, JNothing, JNull, NoTypeHints}
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

object SessionStates {
  val RUNNING = "running"
  val DEAD = "dead"
  val FAIL = "fail"
  val NOT_STARTED = "not_started"
}

/**
 * Represents an interactive session for job and state management.
 *
 * @param applicationId
 *   Unique identifier for the EMR-S application.
 * @param jobId
 *   Identifier for the specific EMR-S job.
 * @param sessionId
 *   Unique session identifier.
 * @param state
 *   Current state of the session.
 * @param lastUpdateTime
 *   Timestamp of the last update.
 * @param jobStartTime
 *   Start time of the job.
 * @param excludedJobIds
 *   List of job IDs that are excluded.
 * @param error
 *   Optional error message.
 * @param sessionContext
 *   Additional context for the session.
 */
class InteractiveSession(
    val applicationId: String,
    val jobId: String,
    val sessionId: String,
    var state: String,
    val lastUpdateTime: Long,
    val jobStartTime: Long = 0,
    val excludedJobIds: Seq[String] = Seq.empty[String],
    val error: Option[String] = None,
    sessionContext: Map[String, Any] = Map.empty[String, Any])
    extends ContextualDataStore {
  context = sessionContext // Initialize the context from the constructor

  def isRunning: Boolean = state == SessionStates.RUNNING
  def isDead: Boolean = state == SessionStates.DEAD
  def isFail: Boolean = state == SessionStates.FAIL
  def isNotStarted: Boolean = state == SessionStates.NOT_STARTED

  override def toString: String = {
    val excludedJobIdsStr = excludedJobIds.mkString("[", ", ", "]")
    val errorStr = error.getOrElse("None")
    // Does not include context, which could contain sensitive information.
    s"FlintInstance(applicationId=$applicationId, jobId=$jobId, sessionId=$sessionId, state=$state, " +
      s"lastUpdateTime=$lastUpdateTime, jobStartTime=$jobStartTime, excludedJobIds=$excludedJobIdsStr, error=$errorStr)"
  }
}

object InteractiveSession {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(job: String): InteractiveSession = {
    val meta = parse(job)
    val applicationId = (meta \ "applicationId").extract[String]
    val state = (meta \ "state").extract[String]
    val jobId = (meta \ "jobId").extract[String]
    val sessionId = (meta \ "sessionId").extract[String]
    val lastUpdateTime = (meta \ "lastUpdateTime").extract[Long]
    val jobStartTime: Long = meta \ "jobStartTime" match {
      case JNothing | JNull => 0L // Default value for missing or null jobStartTime
      case value => value.extract[Long]
    }
    // To handle the possibility of excludeJobIds not being present,
    // we use extractOpt which gives us an Option[Seq[String]].
    // If it is not present, it will return None, which we can then
    // convert to an empty Seq[String] using getOrElse.
    // Replace extractOpt with jsonOption and map
    val excludeJobIds: Seq[String] = meta \ "excludeJobIds" match {
      case JArray(lst) => lst.map(_.extract[String])
      case _ => Seq.empty[String]
    }

    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }

    new InteractiveSession(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError)
  }

  def deserializeFromMap(source: JavaMap[String, AnyRef]): InteractiveSession = {
    // Since we are dealing with JavaMap, we convert it to a Scala mutable Map for ease of use.
    val scalaSource = source.asScala

    val applicationId = scalaSource("applicationId").asInstanceOf[String]
    val state = scalaSource("state").asInstanceOf[String]
    val jobId = scalaSource("jobId").asInstanceOf[String]
    val sessionId = scalaSource("sessionId").asInstanceOf[String]
    val lastUpdateTime = scalaSource("lastUpdateTime").asInstanceOf[Long]

    // Safely extract 'jobStartTime' considering potential null or absence
    val jobStartTime: Long = scalaSource.get("jobStartTime") match {
      case Some(value: java.lang.Long) =>
        value.longValue() // Convert java.lang.Long to Scala Long
      case _ => 0L // Default value if 'jobStartTime' is null or not present
    }

    // We safely handle the possibility of excludeJobIds being absent or not a list.
    val excludeJobIds: Seq[String] = parseExcludedJobIds(scalaSource.get("excludeJobIds"))

    // Handle error similarly, ensuring we get an Option[String].
    val maybeError: Option[String] = scalaSource.get("error") match {
      case Some(str: String) => Some(str)
      case _ => None
    }

    // Construct a new FlintInstance with the extracted values.
    new InteractiveSession(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError)
  }

  /**
   * After the initial setup, the 'jobId' is only readable by Spark, and it should not be
   * overridden. We use 'jobId' to ensure that only one job can run per session. In the case of a
   * new job for the same session, it will override the 'jobId' in the session document. The old
   * job will periodically check the 'jobId.' If the read 'jobId' does not match the current
   * 'jobId,' the old job will exit early. Therefore, it is crucial that old jobs do not overwrite
   * the session store's 'jobId' field after the initial setup.
   *
   * @param job
   *   Flint session object
   * @param currentTime
   *   current timestamp in milliseconds
   * @param includeJobId
   *   flag indicating whether to include the "jobId" field in the serialization
   * @return
   *   serialized Flint session
   */
  def serialize(
      job: InteractiveSession,
      currentTime: Long,
      includeJobId: Boolean = true): String = {
    val baseMap = Map(
      "type" -> "session",
      "sessionId" -> job.sessionId,
      "error" -> job.error.getOrElse(""),
      "applicationId" -> job.applicationId,
      "state" -> job.state,
      // update last update time
      "lastUpdateTime" -> currentTime,
      // Convert a Seq[String] into a comma-separated string, such as "id1,id2".
      // This approach is chosen over serializing to an array format (e.g., ["id1", "id2"])
      // because it simplifies client-side processing. With a comma-separated string,
      // clients can easily ignore this field if it's not in use, avoiding the need
      // for array parsing logic. This makes the serialized data more straightforward to handle.
      "excludeJobIds" -> job.excludedJobIds.mkString(","),
      "jobStartTime" -> job.jobStartTime)

    val resultMap = if (includeJobId) {
      baseMap + ("jobId" -> job.jobId)
    } else {
      baseMap
    }

    Serialization.write(resultMap)
  }

  def serializeWithoutJobId(job: InteractiveSession, currentTime: Long): String = {
    serialize(job, currentTime, includeJobId = false)
  }
  private def parseExcludedJobIds(source: Option[Any]): Seq[String] = {
    source match {
      case Some(s: String) => Seq(s)
      case Some(list: JavaList[_]) => list.asScala.toList.collect { case str: String => str }
      case None => Seq.empty[String]
      case _ =>
        Seq.empty
    }
  }
}
