/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.app

import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.{JInt, JNothing, JString}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

class FlintCommand(
    var state: String,
    val query: String,
    val queryId: String,
    val sessionId: String,
    val submitTime: Long,
    val queryRunTimeSecs: Option[Long],
    val dataSource: String,
    // statementId is the statement type doc id
    val statementId: String,
    val flintIndexName: Option[String],
    var error: Option[String] = None,
    val executionContext: Option[Map[String, Any]] = None) {

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
    val queryId = (meta \ "queryId").extract[String]
    val sessionId = (meta \ "sessionId").extract[String]
    val submitTime = (meta \ "submitTime").extract[Long]
    val queryRunTimeSecs = (meta \ "queryRunTimeSecs") match {
      case JInt(num) => Some(num.toLong)
      case JNothing => None
      case _ => None
    }
    val dataSource = (meta \ "dataSource").extract[String]
    val statementId = (meta \ "statementId").extract[String]
    val flintIndexName = (meta \ "flintIndexName") match {
      case JString(str) => Some(str)
      case JNothing => None
      case _ => None
    }
    val maybeError: Option[String] = (meta \ "error") match {
      case JString(str) => Some(str)
      case JNothing => None
      case _ => None
    }
    val executionContext = (meta \ "executionContext").extract[Option[Map[String, Any]]]

    new FlintCommand(
      state,
      query,
      queryId,
      sessionId,
      submitTime,
      queryRunTimeSecs,
      dataSource,
      statementId,
      flintIndexName,
      maybeError,
      executionContext)
  }

  def serialize(flintCommand: FlintCommand): String = {
    Serialization.write(
      Map(
        "state" -> flintCommand.state,
        "query" -> flintCommand.query,
        "queryId" -> flintCommand.queryId,
        "sessionId" -> flintCommand.sessionId,
        "submitTime" -> flintCommand.submitTime,
        "queryRunTimeSecs" -> flintCommand.queryRunTimeSecs.getOrElse(null),
        "dataSource" -> flintCommand.dataSource,
        "statementId" -> flintCommand.statementId,
        "flintIndexName" -> flintCommand.flintIndexName.getOrElse(null),
        "error" -> flintCommand.error.getOrElse(null),
        "executionContext" -> flintCommand.executionContext.getOrElse(null)))
  }
}
