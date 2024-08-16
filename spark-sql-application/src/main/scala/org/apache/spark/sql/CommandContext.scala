/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

case class CommandContext(
    val spark: SparkSession,
    val dataSource: String,
    val sessionId: String,
    val sessionManager: SessionManager,
    val jobId: String,
    var statementsExecutionManager: StatementExecutionManager,
    val queryResultWriter: QueryResultWriter,
    val queryExecutionTimeout: Duration,
    val inactivityLimitMillis: Long,
    val queryWaitTimeMillis: Long,
    val queryLoopExecutionFrequency: Long)
