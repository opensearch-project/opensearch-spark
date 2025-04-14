/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.common.scheduler.model.LangType
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsSparkListener, MetricsUtil}
import org.opensearch.flint.spark.FlintSpark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.util.ShuffleCleaner
import org.apache.spark.util.ThreadUtils

case class JobOperator(
    applicationId: String,
    jobId: String,
    sparkSession: SparkSession,
    statement: FlintStatement,
    dataSource: String,
    resultIndex: String,
    jobType: String,
    streamingRunningCount: AtomicInteger,
    statementRunningCount: AtomicInteger)
    extends Logging
    with FlintJobExecutor {

  // JVM shutdown hook
  sys.addShutdownHook(stop())
  val isStreaming = jobType.equalsIgnoreCase(FlintJobType.STREAMING)
  val isStreamingOrBatch =
    jobType.equalsIgnoreCase(FlintJobType.STREAMING) || jobType.equalsIgnoreCase(
      FlintJobType.BATCH)
  val isWarmpoolEnabled =
    sparkSession.conf.get(FlintSparkConf.WARMPOOL_ENABLED.key, "false").toBoolean
  val segmentName = getSegmentName(sparkSession)

  def start(): Unit = {
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "check-create-index")
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
    var dataToWrite: Option[DataFrame] = None

    val startTime = System.currentTimeMillis()
    if (isStreamingOrBatch) {
      streamingRunningCount.incrementAndGet()
    } else {
      statementRunningCount.incrementAndGet()
    }

    // osClient needs spark session to be created first to get FlintOptions initialized.
    // Otherwise, we will have connection exception from EMR-S to OS.
    val osClient = new OSClient(FlintSparkConf().flintOptions())

    // QueryResultWriter depends on sessionManager to fetch the sessionContext
    val sessionManager = instantiateSessionManager(sparkSession, Some(resultIndex))

    val commandContext = CommandContext(
      applicationId,
      jobId,
      sparkSession,
      dataSource,
      jobType,
      "", // FlintJob doesn't have sessionId
      sessionManager,
      Duration.Inf, // FlintJob doesn't have queryExecutionTimeout
      -1, // FlintJob doesn't have inactivityLimitMillis
      -1, // FlintJob doesn't have queryWaitTimeMillis
      -1 // FlintJob doesn't have queryLoopExecutionFrequency
    )

    val statementExecutionManager =
      instantiateStatementExecutionManager(commandContext, resultIndex, osClient)

    val readWriteBytesSparkListener = new MetricsSparkListener()
    sparkSession.sparkContext.addSparkListener(readWriteBytesSparkListener)

    try {
      val futurePrepareQueryExecution = Future {
        statementExecutionManager.prepareStatementExecution()
      }
      val data = statementExecutionManager.executeStatement(statement)
      dataToWrite = Some(
        ThreadUtils.awaitResult(futurePrepareQueryExecution, Duration(1, MINUTES)) match {
          case Right(_) => data
          case Left(err) =>
            throwableHandler.setError(err)
            constructErrorDF(
              applicationId,
              jobId,
              sparkSession,
              dataSource,
              "FAILED",
              err,
              statement.queryId,
              statement.query,
              "",
              startTime)
        })
    } catch {
      case e: TimeoutException =>
        throwableHandler.recordThrowable(s"Preparation for query execution timed out", e)
        dataToWrite = Some(
          constructErrorDF(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            "TIMEOUT",
            throwableHandler.error,
            statement.queryId,
            statement.query,
            "",
            startTime))
        incrementCounter(MetricConstants.QUERY_EXECUTION_FAILED_METRIC)
      case t: Throwable =>
        val error = processQueryException(t)
        dataToWrite = Some(
          constructErrorDF(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            "FAILED",
            error,
            statement.queryId,
            statement.query,
            "",
            startTime))
        incrementCounter(MetricConstants.QUERY_EXECUTION_FAILED_METRIC)
    } finally {
      emitTimerMetric(MetricConstants.QUERY_EXECUTION_TIME_METRIC, startTime)
      readWriteBytesSparkListener.emitMetrics()
      sparkSession.sparkContext.removeSparkListener(readWriteBytesSparkListener)

      val resultWriterStartTime = System.currentTimeMillis()
      try {
        dataToWrite.foreach(df => {
          if (isStreamingOrBatch) {
            writeDataFrameToOpensearch(df, resultIndex, osClient)
          } else {
            val queryResultWriter = instantiateQueryResultWriter(sparkSession, commandContext)
            val processedDataFrame = queryResultWriter.processDataFrame(df, statement, startTime)
            queryResultWriter.writeDataFrame(processedDataFrame, statement)
          }
        })
      } catch {
        case t: Throwable =>
          incrementCounter(MetricConstants.RESULT_WRITER_FAILED_METRIC)
          throwableHandler.recordThrowable(
            s"Failed to write to result. Cause='${t.getMessage}', originalError='${throwableHandler.error}'",
            t)
      } finally {
        emitTimerMetric(MetricConstants.QUERY_RESULT_WRITER_TIME_METRIC, resultWriterStartTime)
      }
      if (throwableHandler.hasException) statement.fail() else statement.complete()
      statement.error = Some(throwableHandler.error)

      try {
        statementExecutionManager.updateStatement(statement)
      } catch {
        case t: Throwable =>
          throwableHandler.recordThrowable(
            s"Failed to update statement. Cause='${t.getMessage}', originalError='${throwableHandler.error}'",
            t)
      }
      emitTimerMetric(MetricConstants.QUERY_TOTAL_TIME_METRIC, startTime)
      cleanUpResources(threadPool)
    }
  }

  def cleanUpResources(threadPool: ThreadPoolExecutor): Unit = {
    try {
      // Wait for job complete if no error
      if (!throwableHandler.hasException) {
        // Clean Spark shuffle data after each microBatch.
        sparkSession.streams.addListener(new ShuffleCleaner(sparkSession))

        if (isStreaming) {
          // Await index monitor before the main thread terminates
          new FlintSpark(sparkSession).flintIndexMonitor.awaitMonitor()
        }
      } else {
        logInfo(s"""
                   | Skip job await due to conditions not met:
                   |  - exceptionThrown: ${throwableHandler.hasException}
                   |  - streaming: $isStreaming
                   |  - activeStreams: ${sparkSession.streams.active.mkString(",")}
                   |""".stripMargin)
      }
    } catch {
      case e: Exception => logError("job failed", e)
    }

    try {
      logInfo("Thread pool is being shut down")
      threadPool.shutdown()
      logInfo("shut down thread threadpool")
    } catch {
      case e: Exception => logError("Fail to close threadpool", e)
    }
    recordCompletionStatus(throwableHandler.hasException)

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

  private def emitTimerMetric(metricName: String, startTime: Long): Unit = {
    MetricsUtil
      .addHistoricGauge(resolveMetricName(metricName), System.currentTimeMillis() - startTime)
  }

  def stop(): Unit = {
    Try {
      logInfo("Stopping Spark session")
      sparkSession.stop()
      logInfo("Stopped Spark session")
    } match {
      case Success(_) =>
      case Failure(e) =>
        throwableHandler.recordThrowable("unexpected error while stopping spark session", e)
    }

    // After handling any exceptions from stopping the Spark session,
    // check if there's a stored exception and throw it if it's an UnrecoverableException
    checkAndThrowUnrecoverableExceptions()
  }

  /**
   * Records the completion of a job by updating the appropriate metrics. This method decrements
   * the running metric for jobs and increments either the success or failure metric based on
   * whether an exception was thrown.
   *
   * @param exceptionThrown
   *   Indicates whether an exception was thrown during the job execution.
   */
  private def recordCompletionStatus(exceptionThrown: Boolean): Unit = {
    // Decrement the metric for running jobs as the job is now completing.
    if (streamingRunningCount.get() > 0) {
      streamingRunningCount.decrementAndGet()
    } else if (statementRunningCount.get() > 0) {
      statementRunningCount.decrementAndGet()
    }

    val metric = {
      (exceptionThrown, isStreamingOrBatch) match {
        case (true, true) => MetricConstants.STREAMING_FAILED_METRIC
        case (true, false) => MetricConstants.STATEMENT_FAILED_METRIC
        case (false, true) => MetricConstants.STREAMING_SUCCESS_METRIC
        case (false, false) => MetricConstants.STATEMENT_SUCCESS_METRIC
      }
    }

    if (isWarmpoolEnabled) {
      MetricsUtil.incrementCounter(String.format("%s.%s", segmentName, metric));
    } else {
      MetricsUtil.incrementCounter(metric);
    }
  }

  private def instantiateStatementExecutionManager(
      commandContext: CommandContext,
      resultIndex: String,
      osClient: OSClient): StatementExecutionManager = {
    import commandContext._
    instantiate(
      new SingleStatementExecutionManager(commandContext, resultIndex, osClient),
      spark.conf.get(FlintSparkConf.CUSTOM_STATEMENT_MANAGER.key, ""),
      spark,
      sessionId)
  }

  /**
   * Returns a segment name formatted with the maximum executor count. For example, if the max
   * executor count is 1, the return value will be "1e".
   *
   * @param spark
   *   The Spark session.
   * @return
   *   A string in the format "<maxExecutorsCount>e", e.g., "1e", "2e".
   */
  private def getSegmentName(spark: SparkSession): String = {
    val maxExecutorsCount = spark.conf.get(FlintSparkConf.MAX_EXECUTORS_COUNT.key, "unknown")
    String.format("%se", maxExecutorsCount)
  }

  /**
   * Resolves the full metric name based on the job type and warm pool status. If the warm pool is
   * enabled, the metric name is prefixed with the segment name and job type (STREAMING or
   * STATEMENT).
   *
   * @param metricName
   *   The base metric name to resolve.
   * @return
   *   The resolved metric name, e.g., "1e.streaming.success.count" if warm pool is enabled.
   */
  private def resolveMetricName(metricName: String): String = {
    if (isWarmpoolEnabled) {
      val jobType = if (isStreamingOrBatch) FlintJobType.STREAMING else MetricConstants.STATEMENT
      val newMetricName = String.format("%s.%s.%s", segmentName, jobType, metricName)
      return newMetricName
    }
    metricName
  }

  private def incrementCounter(metricName: String): Unit = {
    MetricsUtil.incrementCounter(resolveMetricName(metricName))
  }
}
