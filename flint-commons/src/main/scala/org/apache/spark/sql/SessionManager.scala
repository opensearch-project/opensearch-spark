/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.{FlintStatement, InteractiveSession}

import org.apache.spark.sql.SessionUpdateMode.SessionUpdateMode

trait SessionManager {
  def getSessionManagerMetadata: Map[String, Any]
  def getSessionDetails(sessionId: String): Option[InteractiveSession]
  def updateSessionDetails(
      sessionDetails: InteractiveSession,
      updateMode: SessionUpdateMode): Unit
  def hasPendingStatement(sessionId: String): Boolean
  def recordHeartbeat(sessionId: String): Unit
}

object SessionUpdateMode extends Enumeration {
  type SessionUpdateMode = Value
  val Update, Upsert, UpdateIf = Value
}
