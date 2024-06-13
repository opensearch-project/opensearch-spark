/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintInstance

import org.apache.spark.sql.UpdateMode.UpdateMode

trait SessionManager {
  def getSessionManagerMetadata: Map[String, Any]
  def getSessionDetails(sessionId: String): Option[FlintInstance]
  def updateSessionDetails(sessionDetails: FlintInstance, updateMode: UpdateMode): Unit
  def recordHeartbeat(sessionId: String): Unit

  // TODO: further refactor exclusionCheck and canPickNextCommand
  def exclusionCheck(sessionId: String, excludeJobIds: List[String]): Boolean
  def canPickNextCommand(sessionId: String, jobId: String): Boolean
}

object UpdateMode extends Enumeration {
  type UpdateMode = Value
  val Update, Upsert, UpdateIf = Value
}
