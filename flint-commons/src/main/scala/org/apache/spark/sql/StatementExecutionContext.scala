/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.duration.Duration

case class StatementExecutionContext(
    spark: SparkSession,
    jobId: String,
    sessionId: String,
    sessionManager: SessionManager,
    statementLifecycleManager: StatementLifecycleManager,
    queryResultWriter: QueryResultWriter,
    dataSource: String,
    queryExecutionTimeout: Duration,
    inactivityLimitMillis: Long,
    queryWaitTimeMillis: Long)
