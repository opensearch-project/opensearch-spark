/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.codahale.metrics.Timer
import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.logging.CustomLogging
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}
import org.opensearch.flint.core.metrics.MetricsUtil.{getTimerContext, registerGauge, stopTimer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.WarmpoolJobConfConstants._
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.ThreadUtils

object WarmpoolJobConfConstants {
  val MAPPING_CHECK_TIMEOUT = Duration(1, MINUTES)
  val DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS = 10 * 60 * 1000
  val DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY = 100L
}

case class WarmpoolJob(
    conf: SparkConf,
    sparkSession: SparkSession,
    resultIndexOption: Option[String])
    extends Logging
    with FlintJobExecutor {

  private val statementRunningCount = new AtomicInteger(0)
  private val streamingRunningCount = new AtomicInteger(0)
  private val segmentName = getSegmentName(sparkSession)

  def start(): Unit = {
    // Read the values from the Spark configuration or fall back to the default values
    val inactivityLimitMillis: Long =
      conf.getLong(
        FlintSparkConf.REPL_INACTIVITY_TIMEOUT_MILLIS.key,
        FlintOptions.DEFAULT_INACTIVITY_LIMIT_MILLIS)
    val queryWaitTimeoutMillis: Long =
      conf.getLong("spark.flint.job.queryWaitTimeoutMillis", DEFAULT_QUERY_WAIT_TIMEOUT_MILLIS)
    val queryLoopExecutionFrequency: Long =
      conf.getLong(
        "spark.flint.job.queryLoopExecutionFrequency",
        DEFAULT_QUERY_LOOP_EXECUTION_FREQUENCY)

    val sessionManager = instantiateSessionManager(sparkSession, resultIndexOption)
    val commandContext = CommandContext(
      environmentProvider.getEnvVar("SERVERLESS_EMR_VIRTUAL_CLUSTER_ID", "unknown"),
      environmentProvider.getEnvVar("SERVERLESS_EMR_JOB_ID", "unknown"),
      sparkSession,
      "", // In WP flow, datasource is not known yet
      "", // In WP flow, jobType is not know yet
      "", // WP doesn't use sessionId
      sessionManager,
      Duration.Inf, // WP doesn't have queryExecutionTimeout
      inactivityLimitMillis,
      queryWaitTimeoutMillis, // Used only for interactive queries
      queryLoopExecutionFrequency)
    registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
    try {
      FlintREPL.exponentialBackoffRetry(maxRetries = 5, initialDelay = 2.seconds) {
        queryLoop(commandContext)
      }
    } finally {
      sparkSession.stop()

      // After handling any exceptions from stopping the Spark session,
      // check if there's a stored exception and throw it if it's an UnrecoverableException
      checkAndThrowUnrecoverableExceptions()

      // Check for non-daemon threads that may prevent the driver from shutting down.
      // Non-daemon threads other than the main thread indicate that the driver is still processing tasks,
      // which may be due to unresolved bugs in dependencies or threads not being properly shut down.
      if (terminateJVM && threadPoolFactory.hasNonDaemonThreadsOtherThanMain) {
        logInfo("A non-daemon thread in the driver is seen.")
        // Exit the JVM to prevent resource leaks and potential emr-s job hung.
        // A zero status code is used for a graceful shutdown without indicating an error.
        // If exiting with non-zero status, emr-s job will fail.
        // This is a part of the fault tolerance mechanism to handle such scenarios gracefully
        System.exit(0)
      }
    }
  }

  def queryLoop(commandContext: CommandContext): Unit = {
    import commandContext._

    val statementExecutionManager = instantiateStatementExecutionManager(commandContext)
    var canProceed = true

    try {
      var lastActivityTime = currentTimeProvider.currentEpochMillis()

      while (currentTimeProvider
          .currentEpochMillis() - lastActivityTime <= commandContext.inactivityLimitMillis && canProceed) {
        statementExecutionManager.getNextStatement() match {
          case Some(flintStatement) =>
            flintStatement.running()
            statementExecutionManager.updateStatement(flintStatement)

            val jobType = spark.conf.get(FlintSparkConf.JOB_TYPE.key, FlintJobType.BATCH)
            val dataSource = spark.conf.get(FlintSparkConf.DATA_SOURCE_NAME.key)
            val resultIndex = spark.conf.get(FlintSparkConf.RESULT_INDEX.key)

            val postQuerySelectionCommandContext =
              commandContext.copy(dataSource = dataSource, jobType = jobType)

            CustomLogging.logInfo(s"""Job type is: ${jobType}""")
            val queryResultWriter = instantiateQueryResultWriter(spark, commandContext)

            if (jobType.equalsIgnoreCase(FlintJobType.STREAMING) || jobType.equalsIgnoreCase(
                FlintJobType.BATCH)) {
              processStreamingJob(
                applicationId,
                jobId,
                flintStatement.query,
                flintStatement.queryId,
                dataSource,
                resultIndex,
                jobType,
                spark,
                flintStatement.context)
            } else {
              processInteractiveJob(
                spark,
                postQuerySelectionCommandContext,
                flintStatement,
                statementExecutionManager,
                queryResultWriter)

              // Last query finish time is last activity time
              lastActivityTime = currentTimeProvider.currentEpochMillis()
            }

          case _ =>
            canProceed = false
        }
      }
    } catch {
      case t: Throwable =>
        throwableHandler.recordThrowable(s"Query loop execution failed.", t)
        throw t
    } finally {
      statementExecutionManager.terminateStatementExecution()
    }

    Thread.sleep(commandContext.queryLoopExecutionFrequency)
  }

  private def processStreamingJob(
      applicationId: String,
      jobId: String,
      query: String,
      queryId: String,
      dataSource: String,
      resultIndex: String,
      jobType: String,
      sparkSession: SparkSession,
      executionContext: Map[String, Any]): Unit = {

    // https://github.com/opensearch-project/opensearch-spark/issues/138
    /*
     * To execute queries such as `CREATE SKIPPING INDEX ON my_glue1.default.http_logs_plain (`@timestamp` VALUE_SET) WITH (auto_refresh = true)`,
     * it's necessary to set `spark.sql.defaultCatalog=my_glue1`. This is because AWS Glue uses a single database (default) and table (http_logs_plain),
     * and we need to configure Spark to recognize `my_glue1` as a reference to AWS Glue's database and table.
     * By doing this, we effectively map `my_glue1` to AWS Glue, allowing Spark to resolve the database and table names correctly.
     * Without this setup, Spark would not recognize names in the format `my_glue1.default`.
     */
    sparkSession.conf.set("spark.sql.defaultCatalog", dataSource)

    val streamingRunningCount = new AtomicInteger(0)
    val jobOperator = JobOperator(
      applicationId,
      jobId,
      sparkSession,
      query,
      queryId,
      dataSource,
      resultIndex,
      jobType,
      streamingRunningCount,
      executionContext)

    registerGauge(MetricConstants.STREAMING_RUNNING_METRIC, streamingRunningCount)
    jobOperator.start()
  }

  private def processInteractiveJob(
      sparkSession: SparkSession,
      commandContext: CommandContext,
      flintStatement: FlintStatement,
      statementExecutionManager: StatementExecutionManager,
      queryResultWriter: QueryResultWriter): Unit = {

    import commandContext._

    var dataToWrite: Option[DataFrame] = None
    val startTime: Long = currentTimeProvider.currentEpochMillis()

    statementRunningCount.incrementAndGet()

    val statementTimerContext = getTimerContext(MetricConstants.STATEMENT_PROCESSING_TIME_METRIC)
    implicit val ec: ExecutionContext = ExecutionContext.global

    val futurePrepareQueryExecution = Future {
      statementExecutionManager.prepareStatementExecution()
    }

    try {
      ThreadUtils.awaitResult(futurePrepareQueryExecution, MAPPING_CHECK_TIMEOUT) match {
        case Right(_) =>
          dataToWrite = executeAndHandleInteractiveJob(
            sparkSession,
            commandContext,
            flintStatement,
            startTime,
            statementExecutionManager,
            queryResultWriter)

        case Left(error) =>
          dataToWrite = Some(
            handleCommandFailureAndGetFailedData(
              applicationId,
              jobId,
              sparkSession,
              dataSource,
              error,
              flintStatement,
              "",
              startTime))
      }
    } catch {
      case e: TimeoutException =>
        val error = s"Query execution preparation timed out"
        CustomLogging.logError(error, e)
        dataToWrite = Some(
          handleCommandTimeout(
            applicationId,
            jobId,
            spark,
            dataSource,
            error,
            flintStatement,
            "",
            startTime))

      case NonFatal(e) =>
        val error = s"An unexpected error occurred: ${e.getMessage}"
        throwableHandler.recordThrowable(error, e)
        dataToWrite = Some(
          handleCommandFailureAndGetFailedData(
            applicationId,
            jobId,
            spark,
            dataSource,
            error,
            flintStatement,
            sessionId,
            startTime))
    } finally {
      emitTimeMetric(startTime, MetricConstants.STATEMENT_QUERY_EXECUTION_TIME_METRIC)
      finalizeCommand(
        statementExecutionManager,
        queryResultWriter,
        dataToWrite,
        flintStatement,
        statementTimerContext)
      emitTimeMetric(startTime, MetricConstants.STATEMENT_QUERY_TOTAL_TIME_METRIC)
    }
  }

  private def executeAndHandleInteractiveJob(
      sparkSession: SparkSession,
      commandContext: CommandContext,
      flintStatement: FlintStatement,
      startTime: Long,
      statementExecutionManager: StatementExecutionManager,
      queryResultWriter: QueryResultWriter): Option[DataFrame] = {

    import commandContext._

    try {
      if (currentTimeProvider
          .currentEpochMillis() - flintStatement.submitTime > queryWaitTimeMillis) {
        Some(
          handleCommandFailureAndGetFailedData(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            "wait timeout",
            flintStatement,
            "", // WP doesn't use sessionId
            startTime))
      } else {
        // Execute the statement and get the resulting DataFrame
        val df = statementExecutionManager.executeStatement(flintStatement)
        // Process the DataFrame, applying any necessary transformations
        // and triggering Spark actions to materialize the results
        Some(queryResultWriter.processDataFrame(df, flintStatement, startTime))
      }
    } catch {
      case e: TimeoutException =>
        incrementCounter(MetricConstants.STATEMENT_EXECUTION_FAILED_METRIC)
        val error = s"Query execution preparation timed out"
        CustomLogging.logError(error, e)
        Some(
          handleCommandTimeout(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "", // WP doesn't use sessionId
            startTime))

      case t: Throwable =>
        incrementCounter(MetricConstants.STATEMENT_EXECUTION_FAILED_METRIC)
        val error = FlintREPL.processQueryException(t, flintStatement)
        CustomLogging.logError(error, t)
        Some(
          handleCommandFailureAndGetFailedData(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            error,
            flintStatement,
            "", // WP doesn't use sessionId
            startTime))
    }
  }

  /**
   * Finalize statement after processing
   *
   * @param dataToWrite
   *   Data to write
   * @param flintStatement
   *   Flint statement
   */
  private def finalizeCommand(
      statementExecutionManager: StatementExecutionManager,
      queryResultWriter: QueryResultWriter,
      dataToWrite: Option[DataFrame],
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context): Unit = {

    val resultWriterStartTime: Long = currentTimeProvider.currentEpochMillis()

    try {
      dataToWrite.foreach(df => queryResultWriter.writeDataFrame(df, flintStatement))

      if (flintStatement.isRunning || flintStatement.isWaiting) {
        flintStatement.complete()
      }
    } catch {
      case t: Throwable =>
        incrementCounter(MetricConstants.STATEMENT_RESULT_WRITER_FAILED_METRIC)
        val error =
          s"""Fail to write result of ${flintStatement}, cause: ${throwableHandler.error}"""
        throwableHandler.recordThrowable(error, t)
        CustomLogging.logError(error, t)
        flintStatement.fail()
    } finally {
      if (throwableHandler.hasException) flintStatement.fail() else flintStatement.complete()
      flintStatement.error = Some(throwableHandler.error)

      emitTimeMetric(resultWriterStartTime, MetricConstants.STATEMENT_RESULT_WRITER_TIME_METRIC)
      statementExecutionManager.updateStatement(flintStatement)
      recordStatementStateChange(flintStatement, statementTimerContext)
    }
  }

  private def emitTimeMetric(startTime: Long, metricName: String): Unit = {
    val metricNameWithSegment = String.format("%s.%s", segmentName, metricName)
    MetricsUtil.addHistoricGauge(metricNameWithSegment, System.currentTimeMillis() - startTime)
  }

  private def recordStatementStateChange(
      flintStatement: FlintStatement,
      statementTimerContext: Timer.Context): Unit = {
    stopTimer(statementTimerContext)
    if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }
    if (flintStatement.isComplete) {
      incrementCounter(MetricConstants.STATEMENT_SUCCESS_METRIC)
    } else if (flintStatement.isFailed) {
      incrementCounter(MetricConstants.STATEMENT_FAILED_METRIC)
    }
  }

  private def incrementCounter(metricName: String) {
    val metricWithSegmentName = String.format("%s.%s", segmentName, metricName)
    MetricsUtil.incrementCounter(metricWithSegmentName)
  }
}
