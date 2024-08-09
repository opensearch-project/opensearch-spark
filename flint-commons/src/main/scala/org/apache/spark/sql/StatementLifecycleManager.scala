/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

/**
 * Trait defining the interface for managing the lifecycle of executing a FlintStatement.
 */
trait StatementLifecycleManager {

  /**
   * Prepares the statement lifecycle.
   */
  def prepareStatementLifecycle(): Either[String, Unit]

//  def executeStatement(statement: FlintStatement): DataFrame

  /**
   * Updates a specific statement.
   */
  def updateStatement(statement: FlintStatement): Unit

  /**
   * Terminates the statement lifecycle.
   */
  def terminateStatementLifecycle(): Unit
}
