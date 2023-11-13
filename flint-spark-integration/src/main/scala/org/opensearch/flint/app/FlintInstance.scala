/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

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
    // We need jobStartTime to check if HMAC token is expired or not
    val jobStartTime: Long = 0,
    val excludedJobIds: Seq[String] = Seq.empty[String],
    val error: Option[String] = None) {}

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

    new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError)
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

    // Construct a new FlintInstance with the extracted values.
    new FlintInstance(
      applicationId,
      jobId,
      sessionId,
      state,
      lastUpdateTime,
      jobStartTime,
      excludeJobIds,
      maybeError)
  }

  def serialize(job: FlintInstance, currentTime: Long): String = {
    // jobId is only readable by spark, thus we don't override jobId
    Serialization.write(
      Map(
        "type" -> "session",
        "sessionId" -> job.sessionId,
        "error" -> job.error.getOrElse(""),
        "applicationId" -> job.applicationId,
        "state" -> job.state,
        // update last update time
        "lastUpdateTime" -> currentTime,
        "excludeJobIds" -> job.excludedJobIds,
        "jobStartTime" -> job.jobStartTime))
  }
}
