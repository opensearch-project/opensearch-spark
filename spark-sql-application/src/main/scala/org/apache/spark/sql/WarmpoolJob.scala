/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.common.scheduler.model.LangType
import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.registerGauge

import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.currentTimeProvider
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * This class executes Spark jobs in "warm pool" mode, repeatedly calling the client to fetch
 * query details (job type, data source, configurations). The job is created without any
 * query-specific configurations, and the client sets the Spark configurations at runtime during
 * each iteration
 */
case class WarmpoolJob(
    applicationId: String,
    jobId: String,
    spark: SparkSession,
    streamingRunningCount: AtomicInteger,
    statementRunningCount: AtomicInteger)
    extends Logging
    with FlintJobExecutor {

  def start(): Unit = {
    val commandContext = CommandContext(
      applicationId,
      jobId,
      spark,
      "", // datasource is not known yet
      "", // jobType is not known yet
      "", // WP doesn't have sessionId
      null, // WP doesn't use SessionManager
      Duration.Inf, // WP doesn't have queryExecutionTimeout
      -1, // WP doesn't have inactivityLimitMillis
      -1, // WP doesn't have queryWaitTimeMillis
      -1 // WP doesn't have queryLoopExecutionFrequency
    )
    registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
    registerGauge(MetricConstants.STATEMENT_RUNNING_METRIC, statementRunningCount)

    val statementExecutionManager =
      instantiateStatementExecutionManager(commandContext)

    queryLoop(statementExecutionManager)
  }

  /**
   * Executes statements from the StatementExecutionManager in a loop until no more statements are
   * available.
   */
  def queryLoop(statementExecutionManager: StatementExecutionManager): Unit = {
    var canProceed = true

    try {
      while (canProceed) {
        statementExecutionManager.getNextStatement() match {
          case Some(flintStatement) =>
            flintStatement.running()
            statementExecutionManager.updateStatement(flintStatement)

            val jobType = spark.conf.get(FlintSparkConf.JOB_TYPE.key, FlintJobType.BATCH)
            val dataSource = spark.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key)
            val resultIndex = spark.conf.get(FlintSparkConf.RESULT_INDEX.key)
            val jobOperator = createJobOperator(
              spark,
              applicationId,
              jobId,
              flintStatement,
              dataSource,
              resultIndex,
              jobType,
              streamingRunningCount,
              statementRunningCount)

            // The client sets this Spark configuration at runtime for each iteration
            // to control whether the JVM should be terminated after the query execution.
            jobOperator.terminateJVM =
              spark.conf.get(FlintSparkConf.TERMINATE_JVM.key, "true").toBoolean
            jobOperator.start()

          case _ =>
            canProceed = false
        }
      }
    } catch {
      case t: Throwable =>
        // Record and rethrow in query loop
        throwableHandler.recordThrowable(s"Query loop execution failed.", t)
        throw t
    }
  }
}
