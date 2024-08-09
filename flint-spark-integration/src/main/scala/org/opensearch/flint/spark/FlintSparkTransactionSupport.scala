/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.common.metadata.log.{FlintMetadataLogService, OptimisticTransaction}
import org.opensearch.flint.common.metadata.log.OptimisticTransaction.NO_LOG_ENTRY
import org.opensearch.flint.core.FlintClient

import org.apache.spark.internal.Logging

/**
 * Provides transaction support with proper error handling and logging capabilities.
 *
 * @note
 *   This trait requires the mixing class to provide both `FlintClient` and
 *   `FlintMetadataLogService` so this trait can help create transaction context.
 */
trait FlintSparkTransactionSupport extends Logging {

  /** Flint client defined in the mixing class */
  protected def flintClient: FlintClient

  /** Flint metadata log service defined in the mixing class */
  protected def flintMetadataLogService: FlintMetadataLogService

  /**
   * Executes a block of code within a transaction context, handling and logging errors
   * appropriately. This method logs the start and completion of the transaction and captures any
   * exceptions that occur, enriching them with detailed error messages before re-throwing. If the
   * index data is missing (excluding index creation actions), the operation is bypassed, and any
   * dangling metadata log entries are cleaned up.
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
   *   Some(result) of the operation block if the operation is executed, or None if the operation
   *   execution is bypassed due to missing index data
   */
  def withTransaction[T](indexName: String, opName: String, forceInit: Boolean = false)(
      opBlock: OptimisticTransaction[T] => T): Option[T] = {
    logInfo(s"Starting index operation [$opName $indexName] with forceInit=$forceInit")
    try {
      // Execute the action if data index exists or create index action (indicated by forceInit)
      if (forceInit || flintClient.exists(indexName)) {

        // Create transaction (only have side effect if forceInit is true)
        val tx: OptimisticTransaction[T] =
          flintMetadataLogService.startTransaction(indexName, forceInit)
        val result = opBlock(tx)
        logInfo(s"Index operation [$opName $indexName] complete")
        Some(result)
      } else {
        /*
         * If execution reaches this point, it indicates that the Flint index is corrupted.
         * In such cases, clean up the metadata log, as the index data no longer exists.
         * There is a very small possibility that users may recreate the index in the
         * interim, but metadata log get deleted by this cleanup process.
         */
        logWarning(
          s"Bypassing index operation [$opName $indexName] as index data has been deleted")
        flintMetadataLogService
          .startTransaction(indexName)
          .initialLog(_ => true)
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {})
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to execute index operation [$opName $indexName]", e)

        // Rethrowing the original exception for high level logic to handle
        throw e
    }
  }
}
