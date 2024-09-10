/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

/**
 * Trait defining the interface for managing FlintStatement execution. For example, in FlintREPL,
 * multiple FlintStatements are running in a micro-batch within same session.
 *
 * This interface can also apply to other spark entry point like FlintJob.
 */
trait StatementExecutionManager {

  /**
   * Prepares execution of each individual statement
   */
  def prepareStatementExecution(): Either[String, Unit]

  /**
   * Executes a specific statement and returns the spark dataframe
   */
  def executeStatement(statement: FlintStatement): DataFrame

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
  def terminateStatementExecution(): Unit
}
