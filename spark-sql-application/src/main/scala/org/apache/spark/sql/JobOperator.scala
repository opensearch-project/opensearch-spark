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
import org.opensearch.flint.core.metrics.MetricsUtil.incrementCounter
import org.opensearch.flint.spark.FlintSpark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.util.ShuffleCleaner
import org.apache.spark.util.ThreadUtils

case class JobOperator(
    applicationId: String,
    jobId: String,
    sparkSession: SparkSession,
    query: String,
    queryId: String,
    dataSource: String,
    resultIndex: String,
    jobType: String,
    streamingRunningCount: AtomicInteger)
    extends Logging
    with FlintJobExecutor {

  // JVM shutdown hook
  sys.addShutdownHook(stop())

  def start(): Unit = {
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "check-create-index")
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    var dataToWrite: Option[DataFrame] = None

    val startTime = System.currentTimeMillis()
    streamingRunningCount.incrementAndGet()

    // osClient needs spark session to be created first to get FlintOptions initialized.
    // Otherwise, we will have connection exception from EMR-S to OS.
    val osClient = new OSClient(FlintSparkConf().flintOptions())

    // TODO: Update FlintJob to Support All Query Types. Track on https://github.com/opensearch-project/opensearch-spark/issues/633
    val commandContext = CommandContext(
      applicationId,
      jobId,
      sparkSession,
      dataSource,
      jobType,
      "", // FlintJob doesn't have sessionId
      null, // FlintJob doesn't have SessionManager
      Duration.Inf, // FlintJob doesn't have queryExecutionTimeout
      -1, // FlintJob doesn't have inactivityLimitMillis
      -1, // FlintJob doesn't have queryWaitTimeMillis
      -1 // FlintJob doesn't have queryLoopExecutionFrequency
    )

    val statementExecutionManager =
      instantiateStatementExecutionManager(commandContext, resultIndex, osClient)

    val readWriteBytesSparkListener = new MetricsSparkListener()
    sparkSession.sparkContext.addSparkListener(readWriteBytesSparkListener)

    val statement =
      new FlintStatement(
        "running",
        query,
        "",
        queryId,
        LangType.SQL,
        currentTimeProvider.currentEpochMillis())

    var exceptionThrown = true
    var error: String = null

    try {
      val futurePrepareQueryExecution = Future {
        statementExecutionManager.prepareStatementExecution()
      }
      val data = statementExecutionManager.executeStatement(statement)
      dataToWrite = Some(
        ThreadUtils.awaitResult(futurePrepareQueryExecution, Duration(1, MINUTES)) match {
          case Right(_) => data
          case Left(err) =>
            error = err
            constructErrorDF(
              applicationId,
              jobId,
              sparkSession,
              dataSource,
              "FAILED",
              err,
              queryId,
              query,
              "",
              startTime)
        })
      exceptionThrown = false
    } catch {
      case e: TimeoutException =>
        error = s"Preparation for query execution timed out"
        logError(error, e)
        dataToWrite = Some(
          constructErrorDF(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            "TIMEOUT",
            error,
            queryId,
            query,
            "",
            startTime))
      case e: Exception =>
        val error = processQueryException(e)
        dataToWrite = Some(
          constructErrorDF(
            applicationId,
            jobId,
            sparkSession,
            dataSource,
            "FAILED",
            error,
            queryId,
            query,
            "",
            startTime))
    } finally {
      emitQueryExecutionTimeMetric(startTime)
      readWriteBytesSparkListener.emitMetrics()
      sparkSession.sparkContext.removeSparkListener(readWriteBytesSparkListener)

      try {
        dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
      } catch {
        case e: Exception =>
          exceptionThrown = true
          error = s"Failed to write to result index. originalError='${error}'"
          logError(error, e)
      }
      if (exceptionThrown) statement.fail() else statement.complete()
      statement.error = Some(error)
      statementExecutionManager.updateStatement(statement)

      cleanUpResources(exceptionThrown, threadPool, startTime)
    }
  }

  def cleanUpResources(
      exceptionThrown: Boolean,
      threadPool: ThreadPoolExecutor,
      startTime: Long): Unit = {
    val isStreaming = jobType.equalsIgnoreCase(FlintJobType.STREAMING)
    try {
      // Wait for streaming job complete if no error
      if (!exceptionThrown && isStreaming) {
        // Clean Spark shuffle data after each microBatch.
        sparkSession.streams.addListener(new ShuffleCleaner(sparkSession))
        // Await index monitor before the main thread terminates
        new FlintSpark(sparkSession).flintIndexMonitor.awaitMonitor()
      } else {
        logInfo(s"""
           | Skip streaming job await due to conditions not met:
           |  - exceptionThrown: $exceptionThrown
           |  - streaming: $isStreaming
           |  - activeStreams: ${sparkSession.streams.active.mkString(",")}
           |""".stripMargin)
      }
    } catch {
      case e: Exception => logError("streaming job failed", e)
    }

    try {
      logInfo("Thread pool is being shut down")
      threadPool.shutdown()
      logInfo("shut down thread threadpool")
    } catch {
      case e: Exception => logError("Fail to close threadpool", e)
    }
    recordStreamingCompletionStatus(exceptionThrown)

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

  private def emitQueryExecutionTimeMetric(startTime: Long): Unit = {
    MetricsUtil
      .addHistoricGauge(
        MetricConstants.QUERY_EXECUTION_TIME_METRIC,
        System.currentTimeMillis() - startTime)
  }

  def stop(): Unit = {
    Try {
      logInfo("Stopping Spark session")
      sparkSession.stop()
      logInfo("Stopped Spark session")
    } match {
      case Success(_) =>
      case Failure(e) => logError("unexpected error while stopping spark session", e)
    }
  }

  /**
   * Records the completion of a streaming job by updating the appropriate metrics. This method
   * decrements the running metric for streaming jobs and increments either the success or failure
   * metric based on whether an exception was thrown.
   *
   * @param exceptionThrown
   *   Indicates whether an exception was thrown during the streaming job execution.
   */
  private def recordStreamingCompletionStatus(exceptionThrown: Boolean): Unit = {
    // Decrement the metric for running streaming jobs as the job is now completing.
    if (streamingRunningCount.get() > 0) {
      streamingRunningCount.decrementAndGet()
    }

    exceptionThrown match {
      case true => incrementCounter(MetricConstants.STREAMING_FAILED_METRIC)
      case false => incrementCounter(MetricConstants.STREAMING_SUCCESS_METRIC)
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
}
