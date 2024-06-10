/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.data

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

object CommandStates {
  val RUNNING = "running"
  val SUCCESS = "success"
  val FAILED = "failed"
  val WAITING = "waiting"
}

/**
 * Represents a command processed in the Flint job.
 *
 * @param state
 *   The current state of the command.
 * @param query
 *   SQL-like query string that the command will execute.
 * @param statementId
 *   Unique identifier for the type of statement.
 * @param queryId
 *   Unique identifier for the query.
 * @param submitTime
 *   Timestamp when the command was submitted.
 * @param error
 *   Optional error message if the command fails.
 * @param commandContext
 *   Additional context for the command as key-value pairs.
 */
class FlintCommand(
    var state: String,
    val query: String,
    // statementId is the statement type doc id
    val statementId: String,
    val queryId: String,
    val submitTime: Long,
    var error: Option[String] = None,
    commandContext: Map[String, Any] = Map.empty[String, Any])
    extends ContextualData {
  context = commandContext

  def running(): Unit = state = CommandStates.RUNNING
  def complete(): Unit = state = CommandStates.SUCCESS
  def fail(): Unit = state = CommandStates.FAILED
  def isRunning: Boolean = state == CommandStates.RUNNING
  def isComplete: Boolean = state == CommandStates.SUCCESS
  def isFailed: Boolean = state == CommandStates.FAILED
  def isWaiting: Boolean = state == CommandStates.WAITING

  // Does not include context, which could contain sensitive information.
  override def toString: String =
    s"FlintCommand(state=$state, query=$query, statementId=$statementId, queryId=$queryId, submitTime=$submitTime, error=$error)"
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
