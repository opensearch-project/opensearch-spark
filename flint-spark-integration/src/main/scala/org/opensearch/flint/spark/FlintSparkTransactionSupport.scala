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

  /** Abstract FlintClient that need to be defined in the mixing class */
  protected def flintClient: FlintClient

  /** Abstract data source name that need to be defined in the mixing class */
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
    logInfo(s"Starting index operation [$opName for $indexName] with forceInit=$forceInit")
    try {
      // Create transaction (only have side effect if forceInit is true)
      val tx: OptimisticTransaction[T] =
        flintClient.startTransaction(indexName, dataSourceName, forceInit)

      val result = opBlock(tx)
      logInfo(s"Index operation [$opName] complete")
      result
    } catch {
      case e: Exception =>
        // Extract and add root cause message to final error message
        val rootCauseMessage = extractRootCause(e)
        val detailedMessage =
          s"Failed to execute index operation [$opName] caused by: $rootCauseMessage"
        logError(detailedMessage, e)

        // Re-throw with new detailed error message
        throw new IllegalStateException(detailedMessage)
    }
  }

  private def extractRootCause(e: Throwable): String = {
    var cause = e
    while (cause.getCause != null && cause.getCause != cause) {
      cause = cause.getCause
    }

    if (cause.getLocalizedMessage != null) {
      return cause.getLocalizedMessage
    }
    if (cause.getMessage != null) {
      return cause.getMessage
    }
    cause.toString
  }
}
