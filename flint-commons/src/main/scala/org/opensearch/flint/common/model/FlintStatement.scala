/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.common.model

import java.util.Locale

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

object StatementStates {
  val RUNNING = "running"
  val SUCCESS = "success"
  val FAILED = "failed"
  val TIMEOUT = "timeout"
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
 * @param langType
 *   The language type of the query (e.g., "sql" or "ppl").
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
    val langType: String,
    val submitTime: Long,
    var error: Option[String] = None,
    statementContext: Map[String, Any] = Map.empty[String, Any])
    extends ContextualDataStore {
  context = statementContext

  def running(): Unit = state = StatementStates.RUNNING
  def complete(): Unit = state = StatementStates.SUCCESS
  def fail(): Unit = state = StatementStates.FAILED
  def timeout(): Unit = state = StatementStates.TIMEOUT

  def isRunning: Boolean = state.equalsIgnoreCase(StatementStates.RUNNING)

  def isComplete: Boolean = state.equalsIgnoreCase(StatementStates.SUCCESS)

  def isFailed: Boolean = state.equalsIgnoreCase(StatementStates.FAILED)

  def isWaiting: Boolean = state.equalsIgnoreCase(StatementStates.WAITING)

  def isTimeout: Boolean = state.equalsIgnoreCase(StatementStates.TIMEOUT)

  // Does not include context, which could contain sensitive information.
  override def toString: String =
    s"FlintStatement(state=$state, statementId=$statementId, queryId=$queryId, langType=$langType, submitTime=$submitTime, error=$error)"
}

object FlintStatement {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def deserialize(statement: String): FlintStatement = {
    val meta = parse(statement)
    val state = (meta \ "state").extract[String].toLowerCase(Locale.ROOT)
    val query = (meta \ "query").extract[String]
    val statementId = (meta \ "statementId").extract[String]
    val queryId = (meta \ "queryId").extract[String]
    val langType = (meta \ "lang").extract[String].toLowerCase(Locale.ROOT)
    val submitTime = (meta \ "submitTime").extract[Long]
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case _ => None
    }

    new FlintStatement(state, query, statementId, queryId, langType, submitTime, maybeError)
  }

  def serialize(flintStatement: FlintStatement): String = {
    // we only need to modify state and error
    Serialization.write(
      Map(
        "state" -> flintStatement.state.toLowerCase(Locale.ROOT),
        "error" -> flintStatement.error.getOrElse("")))
  }
}
