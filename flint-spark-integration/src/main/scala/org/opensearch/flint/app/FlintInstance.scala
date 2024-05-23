/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._

import org.json4s.{Formats, JNothing, JNull, NoTypeHints}
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

// lastUpdateTime is added to FlintInstance to track the last update time of the instance. Its unit is millisecond.
class FlintInstance(
    val applicationId: String,
    val jobId: String,
    // sessionId is the session type doc id
    val sessionId: String,
    var state: String,
    val lastUpdateTime: Long,
    val jobStartTime: Long = 0,
    val excludedJobIds: Seq[String] = Seq.empty[String],
    val error: Option[String] = None,
    val currentVersion: Option[Long] = None,
    val executionContext: Option[Map[String, Any]] = None) {
  override def toString: String = {
    val excludedJobIdsStr = excludedJobIds.mkString("[", ", ", "]")
    val errorStr = error.getOrElse("None")
    val currentVersionStr = currentVersion.map(_.toString).getOrElse("None")
    val executionContextStr = executionContext.map(_.toString).getOrElse("None")
    s"FlintInstance(applicationId=$applicationId, jobId=$jobId, sessionId=$sessionId, state=$state, " +
      s"lastUpdateTime=$lastUpdateTime, jobStartTime=$jobStartTime, excludedJobIds=$excludedJobIdsStr, error=$errorStr, " +
      s"currentVersion=$currentVersionStr, executionContext=$executionContextStr)"
  }
}

object FlintInstance {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(job: String): FlintInstance = {
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
    val excludeJobIds: Seq[String] = meta \ "excludeJobIds" match {
      case JArray(lst) => lst.map(_.extract[String])
      case _ => Seq.empty[String]
    }
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }
    val currentVersion: Option[Long] = (meta \ "currentVersion") match {
      case JNothing | JNull => None
      case value => Some(value.extract[Long])
    }
    val executionContext: Option[Map[String, Any]] = (meta \ "executionContext") match {
      case JNothing | JNull => None
      case value => Some(value.extract[Map[String, Any]])
    }

    new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError,
      currentVersion,
      executionContext)
  }

  def deserializeFromMap(source: JavaMap[String, AnyRef]): FlintInstance = {
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
    val excludeJobIds: Seq[String] = scalaSource.get("excludeJobIds") match {
      case Some(lst: java.util.List[_]) => lst.asScala.toList.map(_.asInstanceOf[String])
      case _ => Seq.empty[String]
    }

    // Handle error similarly, ensuring we get an Option[String].
    val maybeError: Option[String] = scalaSource.get("error") match {
      case Some(str: String) => Some(str)
      case _ => None
    }

    val currentVersion: Option[Long] = scalaSource.get("currentVersion") match {
      case Some(value: java.lang.Long) => Some(value.longValue())
      case _ => None
    }

    val executionContext: Option[Map[String, Any]] = scalaSource.get("executionContext") match {
      case Some(map: java.util.Map[_, _]) =>
        Some(map.asInstanceOf[java.util.Map[String, Any]].asScala.toMap)
      case _ => None
    }

    // Construct a new FlintInstance with the extracted values.
    new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError,
      currentVersion,
      executionContext)
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
  def serialize(job: FlintInstance, currentTime: Long, includeJobId: Boolean = true): String = {
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
      "jobStartTime" -> job.jobStartTime,
      "currentVersion" -> job.currentVersion.getOrElse(""),
      "executionContext" -> job.executionContext.getOrElse(Map.empty[String, Any]))

    val resultMap = if (includeJobId) {
      baseMap + ("jobId" -> job.jobId)
    } else {
      baseMap
    }

    Serialization.write(resultMap)
  }

  def serializeWithoutJobId(job: FlintInstance, currentTime: Long): String = {
    serialize(job, currentTime, includeJobId = false)
  }
}
