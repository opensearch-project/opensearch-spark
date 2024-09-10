/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

import org.opensearch.flint.core.storage.{FlintReader, OpenSearchUpdater}

case class CommandContext(
    applicationId: String,
    jobId: String,
    spark: SparkSession,
    dataSource: String,
    jobType: String,
    sessionId: String,
    sessionManager: SessionManager,
    queryResultWriter: QueryResultWriter,
    queryExecutionTimeout: Duration,
    inactivityLimitMillis: Long,
    queryWaitTimeMillis: Long,
    queryLoopExecutionFrequency: Long)
