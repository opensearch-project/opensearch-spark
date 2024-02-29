/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}

import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.incrementCounter
import org.opensearch.flint.core.storage.OpenSearchUpdater

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.createSparkSession
import org.apache.spark.sql.FlintREPL.{executeQuery, logInfo, updateFlintInstanceBeforeShutdown}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.util.ThreadUtils

case class JobOperator(
    spark: SparkSession,
    query: String,
    dataSource: String,
    resultIndex: String,
    streaming: Boolean,
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
    var exceptionThrown = true
    try {
      val futureMappingCheck = Future {
        checkAndCreateIndex(osClient, resultIndex)
      }
      val data = executeQuery(spark, query, dataSource, "", "")

      val mappingCheckResult = ThreadUtils.awaitResult(futureMappingCheck, Duration(1, MINUTES))
      dataToWrite = Some(mappingCheckResult match {
        case Right(_) => data
        case Left(error) =>
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider)
      })
      exceptionThrown = false
    } catch {
      case e: TimeoutException =>
        val error = s"Getting the mapping of index $resultIndex timed out"
        logError(error, e)
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
      case e: Exception =>
        val error = processQueryException(e, spark, dataSource, query, "", "")
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
    } finally {
      cleanUpResources(exceptionThrown, threadPool, dataToWrite, resultIndex, osClient)
    }
  }

  def cleanUpResources(
      exceptionThrown: Boolean,
      threadPool: ThreadPoolExecutor,
      dataToWrite: Option[DataFrame],
      resultIndex: String,
      osClient: OSClient): Unit = {
    try {
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
    } catch {
      case e: Exception => logError("fail to write to result index", e)
    }

    try {
      // Wait for streaming job complete if no error and there is streaming job running
      if (!exceptionThrown && streaming && spark.streams.active.nonEmpty) {
        // wait if any child thread to finish before the main thread terminates
        spark.streams.awaitAnyTermination()
      }
    } catch {
      case e: Exception => logError("streaming job failed", e)
    }

    try {
      threadPool.shutdown()
      logInfo("shut down thread threadpool")
    } catch {
      case e: Exception => logError("Fail to close threadpool", e)
    }
    recordStreamingCompletionStatus(exceptionThrown)
  }

  def stop(): Unit = {
    Try {
      spark.stop()
      logInfo("stopped spark session")
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
}
