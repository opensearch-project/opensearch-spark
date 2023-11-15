/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

class FlintCommand(
    var state: String,
    val query: String,
    // statementId is the statement type doc id
    val statementId: String,
    val queryId: String,
    val submitTime: Long,
    var error: Option[String] = None) {
  def running(): Unit = {
    state = "running"
  }

  def complete(): Unit = {
    state = "success"
  }

  def fail(): Unit = {
    state = "failed"
  }

  def isRunning(): Boolean = {
    state == "running"
  }

  def isComplete(): Boolean = {
    state == "success"
  }

  def isFailed(): Boolean = {
    state == "failed"
  }

  def isWaiting(): Boolean = {
    state == "waiting"
  }

  override def toString: String = {
    s"FlintCommand(state=$state, query=$query, statementId=$statementId, queryId=$queryId, submitTime=$submitTime, error=$error)"
  }
}

object FlintCommand {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(command: String): FlintCommand = {
    val meta = parse(command)
    val state = (meta \ "state").extract[String]
    val query = (meta \ "query").extract[String]
    val statementId = (meta \ "statementId").extract[String]
    val queryId = (meta \ "queryId").extract[String]
    val submitTime = (meta \ "submitTime").extract[Long]
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }

    new FlintCommand(state, query, statementId, queryId, submitTime, maybeError)
  }

  def serialize(flintCommand: FlintCommand): String = {
    // we only need to modify state and error
    Serialization.write(
      Map("state" -> flintCommand.state, "error" -> flintCommand.error.getOrElse("")))
  }
}
