/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.core.metadata.log.OptimisticTransaction

import org.apache.spark.internal.Logging

/**
 * Provides transaction support with proper error handling and logging capabilities.
 *
 * @note
 *   This trait requires the mixing class to extend Spark's `Logging` to utilize its logging
 *   functionalities. Meanwhile it needs to provide `FlintClient` and data source name so this
 *   trait can help create transaction context.
 */
trait FlintSparkTransactionSupport { self: Logging =>

  /** Flint client defined in the mixing class */
  protected def flintClient: FlintClient

  /** Data source name defined in the mixing class */
  protected def dataSourceName: String

  /**
   * Executes a block of code within a transaction context, handling and logging errors
   * appropriately. This method logs the start and completion of the transaction and captures any
   * exceptions that occur, enriching them with detailed error messages before re-throwing.
   *
   * @param indexName
   *   the name of the index on which the operation is performed
   * @param opName
   *   the name of the operation, used for logging
   * @param forceInit
   *   a boolean flag indicating whether to force the initialization of the metadata log
   * @param opBlock
   *   the operation block to execute within the transaction context, which takes an
   *   `OptimisticTransaction` and returns a value of type `T`
   * @tparam T
   *   the type of the result produced by the operation block
   * @return
   *   the result of the operation block
   */
  def withTransaction[T](indexName: String, opName: String, forceInit: Boolean = false)(
      opBlock: OptimisticTransaction[T] => T): T = {
    logInfo(s"Starting index operation [$opName $indexName] with forceInit=$forceInit")
    try {
      // Create transaction (only have side effect if forceInit is true)
      val tx: OptimisticTransaction[T] =
        flintClient.startTransaction(indexName, dataSourceName, forceInit)

      val result = opBlock(tx)
      logInfo(s"Index operation [$opName $indexName] complete")
      result
    } catch {
      case e: Exception =>
        logError(s"Failed to execute index operation [$opName $indexName]", e)

        // Rethrowing the original exception for high level logic to handle
        throw e
    }
  }
}
