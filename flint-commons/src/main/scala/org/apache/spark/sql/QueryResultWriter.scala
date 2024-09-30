/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

/**
 * Trait for writing the result of a query execution to an external data storage.
 */
trait QueryResultWriter {

  /**
   * Writes the given DataFrame to an external data storage based on the FlintStatement metadata.
   * This method is responsible for persisting the query results.
   *
   * Note: This method typically involves I/O operations and may trigger Spark actions to
   * materialize the DataFrame if it hasn't been processed yet.
   *
   * @param dataFrame
   *   The DataFrame containing the query results to be written.
   * @param flintStatement
   *   The FlintStatement containing metadata that guides the writing process.
   */
  def writeDataFrame(dataFrame: DataFrame, flintStatement: FlintStatement): Unit

  /**
   * Defines transformations on the given DataFrame and triggers an action to process it. This
   * method applies necessary transformations based on the FlintStatement metadata and executes an
   * action to compute the result.
   *
   * Note: Calling this method will trigger the actual data processing in Spark. If the Spark SQL
   * thread is waiting for the result of a query, termination on the same thread will be blocked
   * until the action completes.
   *
   * @param dataFrame
   *   The DataFrame to be processed.
   * @param flintStatement
   *   The FlintStatement containing statement metadata.
   * @param queryStartTime
   *   The start time of the query execution.
   * @return
   *   The processed DataFrame after applying transformations and executing an action.
   */
  def processDataFrame(
      dataFrame: DataFrame,
      flintStatement: FlintStatement,
      queryStartTime: Long): DataFrame
}
