/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.storage.OpenSearchUpdater
import org.opensearch.search.sort.SortOrder

import org.apache.spark.internal.Logging

/**
 * SingleStatementExecutionManager is an implementation of StatementExecutionManager interface to
 * run single statement
 * @param commandContext
 */
class SingleStatementExecutionManager(
    commandContext: CommandContext,
    resultIndex: String,
    osClient: OSClient)
    extends StatementExecutionManager
    with FlintJobExecutor
    with Logging {

  override def prepareStatementExecution(): Either[String, Unit] = {
    checkAndCreateIndex(osClient, resultIndex)
  }

  override def updateStatement(statement: FlintStatement): Unit = {
    // TODO: Update FlintJob to Support All Query Types. Track on https://github.com/opensearch-project/opensearch-spark/issues/633
  }

  override def terminateStatementExecution(): Unit = {
    // TODO: Update FlintJob to Support All Query Types. Track on https://github.com/opensearch-project/opensearch-spark/issues/633
  }

  override def getNextStatement(): Option[FlintStatement] = {
    // TODO: Update FlintJob to Support All Query Types. Track on https://github.com/opensearch-project/opensearch-spark/issues/633
    None
  }

  override def executeStatement(statement: FlintStatement): DataFrame = {
    import commandContext._
    val isStreaming = jobType.equalsIgnoreCase("streaming")
    executeQuery(
      applicationId,
      jobId,
      spark,
      statement.query,
      dataSource,
      statement.queryId,
      sessionId,
      isStreaming)
  }
}
