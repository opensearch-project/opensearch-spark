/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.index.seqno.SequenceNumbers

// lastUpdateTime is added to FlintInstance to track the last update time of the instance. Its unit is millisecond.
class FlintInstance(
    val applicationId: String,
    val jobId: String,
    // sessionId is the session type doc id
    val sessionId: String,
    val state: String,
    val lastUpdateTime: Long,
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
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }

    new FlintInstance(applicationId, jobId, sessionId, state, lastUpdateTime, maybeError)
  }

  def serialize(job: FlintInstance): String = {
    Serialization.write(
      Map(
        "type" -> "session",
        "sessionId" -> job.sessionId,
        "error" -> job.error.getOrElse(""),
        "applicationId" -> job.applicationId,
        "jobId" -> job.jobId,
        "state" -> job.state,
        // update last update time
        "lastUpdateTime" -> System.currentTimeMillis()))
  }
}
