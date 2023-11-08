/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

// lastUpdateTime is added to FlintInstance to track the last update time of the instance. Its unit is millisecond.
class FlintInstance(
    val applicationId: String,
    val jobId: String,
    // sessionId is the session type doc id
    val sessionId: String,
    val state: String,
    val lastUpdateTime: Long,
    // We need jobStartTime to check if HMAC token is expired or not
    val jobStartTime: Long,
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
    val jobStartTime = (meta \ "jobStartTime").extract[Long]
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
        "jobStartTime" -> job.jobStartTime))
  }
}
