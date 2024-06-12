/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintStatement

trait StatementManager {
  def prepareCommandLifecycle(): Either[String, Unit]
  def initCommandLifecycle(sessionId: String): FlintStatement
  def closeCommandLifecycle(): Unit
  def updateCommandDetails(commandDetails: FlintStatement): Unit
}
