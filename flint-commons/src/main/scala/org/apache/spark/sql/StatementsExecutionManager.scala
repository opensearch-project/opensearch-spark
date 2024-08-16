/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

/**
 * Trait defining the interface for managing FlintStatements executing in a micro-batch within
 * same session.
 */
trait StatementsExecutionManager {

  /**
   * Prepares execution of each individual statement
   */
  def prepareStatementExecution(): Either[String, Unit]

//  def executeStatement(statement: FlintStatement): DataFrame

  /**
   * Retrieves the next statement to be executed.
   */
  def getNextStatement(): Option[FlintStatement]

  /**
   * Updates a specific statement.
   */
  def updateStatement(statement: FlintStatement): Unit

  /**
   * Terminates the statement lifecycle.
   */
  def terminateStatementsExecution(): Unit
}
