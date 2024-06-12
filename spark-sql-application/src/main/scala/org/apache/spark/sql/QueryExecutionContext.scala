/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.concurrent.duration.Duration

case class QueryExecutionContext(
    spark: SparkSession,
    jobId: String,
    sessionId: String,
    sessionManager: SessionManager,
    dataSource: String,
    resultIndex: String,
    queryExecutionTimeout: Duration,
    inactivityLimitMillis: Long,
    queryWaitTimeMillis: Long)
