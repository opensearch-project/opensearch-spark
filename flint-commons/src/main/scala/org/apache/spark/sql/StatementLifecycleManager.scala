/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintStatement

/**
 * Trait defining the interface for managing the lifecycle of statements.
 */
trait StatementLifecycleManager {

  /**
   * Prepares the statement lifecycle.
   */
  def prepareStatementLifecycle(): Either[String, Unit]

  /**
   * Updates a specific statement.
   */
  def updateStatement(statement: FlintStatement): Unit

  /**
   * Terminates the statement lifecycle.
   */
  def terminateStatementLifecycle(): Unit
}
