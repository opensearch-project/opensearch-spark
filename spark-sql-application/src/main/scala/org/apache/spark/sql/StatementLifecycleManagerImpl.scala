/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}
import org.opensearch.flint.data.FlintStatement

import org.apache.spark.internal.Logging

class StatementLifecycleManagerImpl(context: Map[String, Any])
    extends StatementLifecycleManager
    with FlintJobExecutor
    with Logging {

  val resultIndex = context("resultIndex").asInstanceOf[String]
  val osClient = context("osClient").asInstanceOf[OSClient]
  val flintSessionIndexUpdater =
    context("flintSessionIndexUpdater").asInstanceOf[OpenSearchUpdater]
  val flintReader = context("flintReader").asInstanceOf[FlintReader]

  override def prepareStatementLifecycle(): Either[String, Unit] = {
    try {
      val existingSchema = osClient.getIndexMetadata(resultIndex)
      if (!isSuperset(existingSchema, resultIndexMapping)) {
        Left(s"The mapping of $resultIndex is incorrect.")
      } else {
        Right(())
      }
    } catch {
      case e: IllegalStateException
          if e.getCause != null &&
            e.getCause.getMessage.contains("index_not_found_exception") =>
        createResultIndex(osClient, resultIndex, resultIndexMapping)
      case e: InterruptedException =>
        val error = s"Interrupted by the main thread: ${e.getMessage}"
        Thread.currentThread().interrupt() // Preserve the interrupt status
        logError(error, e)
        Left(error)
      case e: Exception =>
        val error = s"Failed to verify existing mapping: ${e.getMessage}"
        logError(error, e)
        Left(error)
    }
  }
  override def updateStatement(statement: FlintStatement): Unit = {
    flintSessionIndexUpdater.update(statement.statementId, FlintStatement.serialize(statement))
  }
  override def terminateStatementLifecycle(): Unit = {
    flintReader.close()
  }
}
