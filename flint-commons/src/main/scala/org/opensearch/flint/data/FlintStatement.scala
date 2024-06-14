/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.data

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

object StatementStates {
  val RUNNING = "running"
  val SUCCESS = "success"
  val FAILED = "failed"
  val WAITING = "waiting"
}

/**
 * Represents a statement processed in the Flint job.
 *
 * @param state
 *   The current state of the statement.
 * @param query
 *   SQL-like query string that the statement will execute.
 * @param statementId
 *   Unique identifier for the type of statement.
 * @param queryId
 *   Unique identifier for the query.
 * @param submitTime
 *   Timestamp when the statement was submitted.
 * @param error
 *   Optional error message if the statement fails.
 * @param statementContext
 *   Additional context for the statement as key-value pairs.
 */
class FlintStatement(
    var state: String,
    val query: String,
    // statementId is the statement type doc id
    val statementId: String,
    val queryId: String,
    val submitTime: Long,
    var queryStartTime: Option[Long] = Some(-1L),
    var error: Option[String] = None,
    statementContext: Map[String, Any] = Map.empty[String, Any])
    extends ContextualDataStore {
  context = statementContext

  def running(): Unit = state = StatementStates.RUNNING
  def complete(): Unit = state = StatementStates.SUCCESS
  def fail(): Unit = state = StatementStates.FAILED
  def isRunning: Boolean = state == StatementStates.RUNNING
  def isComplete: Boolean = state == StatementStates.SUCCESS
  def isFailed: Boolean = state == StatementStates.FAILED
  def isWaiting: Boolean = state == StatementStates.WAITING

  // Does not include context, which could contain sensitive information.
  override def toString: String =
    s"FlintStatement(state=$state, query=$query, statementId=$statementId, queryId=$queryId, submitTime=$submitTime, error=$error)"
}

object FlintStatement {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(statement: String): FlintStatement = {
    val meta = parse(statement)
    val state = (meta \ "state").extract[String]
    val query = (meta \ "query").extract[String]
    val statementId = (meta \ "statementId").extract[String]
    val queryId = (meta \ "queryId").extract[String]
    val submitTime = (meta \ "submitTime").extract[Long]
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }

    new FlintStatement(state, query, statementId, queryId, submitTime, error = maybeError)
  }

  def serialize(flintStatement: FlintStatement): String = {
    // we only need to modify state and error
    Serialization.write(
      Map("state" -> flintStatement.state, "error" -> flintStatement.error.getOrElse("")))
  }
}
