/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintCommand

trait CommandLifecycleManager {
  def setWriter(writer: REPLWriter): Unit
  def prepareCommandLifecycle(): Either[String, Unit]
  def initCommandLifecycle(sessionId: String): FlintCommand
  def closeCommandLifecycle(): Unit
  def hasPendingCommand(sessionId: String): Boolean
  def updateCommandDetails(commandDetails: FlintCommand): Unit
}
