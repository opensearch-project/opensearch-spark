/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.{FlintStatement, InteractiveSession}

import org.apache.spark.sql.SessionUpdateMode.SessionUpdateMode

/**
 * Trait defining the interface for managing interactive sessions.
 */
trait SessionManager {

  /**
   * Retrieves metadata about the session manager.
   */
  def getSessionManagerMetadata: Map[String, Any]

  /**
   * Fetches the details of a specific session.
   */
  def getSessionDetails(sessionId: String): Option[InteractiveSession]

  /**
   * Updates the details of a specific session.
   */
  def updateSessionDetails(
      sessionDetails: InteractiveSession,
      updateMode: SessionUpdateMode): Unit

  /**
   * Retrieves the next statement to be executed in a specific session.
   */
  def getNextStatement(sessionId: String): Option[FlintStatement]

  /**
   * Records a heartbeat for a specific session to indicate it is still active.
   */
  def recordHeartbeat(sessionId: String): Unit
}

object SessionUpdateMode extends Enumeration {
  type SessionUpdateMode = Value
  val UPDATE, UPSERT, UPDATE_IF = Value
}
