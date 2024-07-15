/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.time.Duration
import java.time.temporal.ChronoUnit.SECONDS
import java.util.Collections.singletonList
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.concurrent.{Map, TrieMap}
import scala.sys.addShutdownHook

import dev.failsafe.{Failsafe, RetryPolicy}
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.function.CheckedRunnable
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.{FAILED, REFRESHING}
import org.opensearch.flint.common.metadata.log.FlintMetadataLogService
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.newDaemonThreadPoolScheduledExecutor

/**
 * Flint Spark index state monitor.
 *
 * @param spark
 *   Spark session
 * @param flintMetadataLogService
 *   Flint metadata log service
 */
class FlintSparkIndexMonitor(
    spark: SparkSession,
    flintMetadataLogService: FlintMetadataLogService)
    extends Logging {

  /** Task execution initial delay in seconds */
  private val INITIAL_DELAY_SECONDS = FlintSparkConf().monitorInitialDelaySeconds()

  /** Task execution interval in seconds */
  private val INTERVAL_SECONDS = FlintSparkConf().monitorIntervalSeconds()

  /** Max error count allowed */
  private val MAX_ERROR_COUNT = FlintSparkConf().monitorMaxErrorCount()

  /**
   * Start monitoring task on the given Flint index.
   *
   * @param indexName
   *   Flint index name
   */
  def startMonitor(indexName: String): Unit = {
    logInfo(s"""Starting index monitor for $indexName with configuration:
         | - Initial delay: $INITIAL_DELAY_SECONDS seconds
         | - Interval: $INTERVAL_SECONDS seconds
         | - Max error count: $MAX_ERROR_COUNT
         |""".stripMargin)

    val task = FlintSparkIndexMonitor.executor.scheduleWithFixedDelay(
      new FlintSparkIndexMonitorTask(indexName),
      INITIAL_DELAY_SECONDS, // Delay to ensure final logging is complete first, otherwise version conflicts
      INTERVAL_SECONDS,
      TimeUnit.SECONDS)

    FlintSparkIndexMonitor.indexMonitorTracker.put(indexName, task)
  }

  /**
   * Cancel scheduled task on the given Flint index.
   *
   * @param indexName
   *   Flint index name
   */
  def stopMonitor(indexName: String): Unit = {
    logInfo(s"Cancelling scheduled task for index $indexName")
    // Hack: Don't remove because awaitMonitor API requires Flint index name.
    val task = FlintSparkIndexMonitor.indexMonitorTracker.get(indexName)
    if (task.isDefined) {
      task.get.cancel(true)
    } else {
      logInfo(s"Cannot find scheduled task")
    }
  }

  /**
   * Waits for the termination of a Spark streaming job associated with a specified index name and
   * updates the Flint index state based on the outcome of the job.
   *
   * @param indexName
   *   The name of the index to monitor. If none is specified, the method will default to any
   *   active stream.
   */
  def awaitMonitor(indexName: Option[String] = None): Unit = {
    logInfo(s"Awaiting index monitor for $indexName")

    // Find streaming job for the given index name, otherwise use the first if any
    val job = indexName
      .flatMap(name => spark.streams.active.find(_.name == name))
      .orElse(spark.streams.active.headOption)

    if (job.isDefined) { // must be present after DataFrameWriter.start() called in refreshIndex API
      val name = job.get.name // use streaming job name because indexName maybe None
      logInfo(s"Awaiting streaming job $name until terminated")
      try {

        /**
         * Await termination of the streaming job. Do not transition the index state to ACTIVE
         * post-termination to prevent conflicts with ongoing transactions in DROP/ALTER API
         * operations. It's generally expected that the job will be terminated through a DROP or
         * ALTER operation if no exceptions are thrown.
         */
        job.get.awaitTermination()
        logInfo(s"Streaming job $name terminated without exception")
      } catch {
        case e: Throwable =>
          logError(s"Streaming job $name terminated with exception: ${e.getMessage}")
          retryUpdateIndexStateToFailed(name, exception = Some(e))
      }
    } else {
      logInfo(s"Index monitor for [$indexName] not found.")

      /*
       * Streaming job exits early. Try to find Flint index name in monitor list.
       * Assuming: 1) there are at most 1 entry in the list, otherwise index name
       * must be given upon this method call; 2) this await API must be called for
       * auto refresh index, otherwise index state will be updated mistakenly.
       */
      val name = FlintSparkIndexMonitor.indexMonitorTracker.keys.headOption
      if (name.isDefined) {
        logInfo(s"Found index name in index monitor task list: ${name.get}")
        retryUpdateIndexStateToFailed(name.get, exception = None)
      } else {
        logInfo(s"Index monitor task list is empty")
      }
    }
  }

  /**
   * Index monitor task that encapsulates the execution logic with number of consecutive error
   * tracked.
   *
   * @param indexName
   *   Flint index name
   */
  private class FlintSparkIndexMonitorTask(indexName: String) extends Runnable {

    /** The number of consecutive error */
    private var errorCnt = 0

    override def run(): Unit = {
      logInfo(s"Scheduler trigger index monitor task for $indexName")
      try {
        if (isStreamingJobActive(indexName)) {
          logInfo("Streaming job is still active")
          flintMetadataLogService.recordHeartbeat(indexName)
        } else {
          logError("Streaming job is not active. Cancelling monitor task")
          stopMonitor(indexName)
          logInfo("Index monitor task is cancelled")
        }
        errorCnt = 0 // Reset counter if no error
      } catch {
        case e: Throwable =>
          errorCnt += 1
          logError(s"Failed to update index log entry, consecutive errors: $errorCnt", e)
          MetricsUtil.incrementCounter(MetricConstants.STREAMING_HEARTBEAT_FAILED_METRIC)

          // Stop streaming job and its monitor if max retry limit reached
          if (errorCnt >= MAX_ERROR_COUNT) {
            logInfo(s"Terminating streaming job and index monitor for $indexName")
            stopStreamingJob(indexName)
            stopMonitor(indexName)
            logInfo(s"Streaming job and index monitor terminated")
          }
      }
    }
  }

  private def isStreamingJobActive(indexName: String): Boolean =
    spark.streams.active.exists(_.name == indexName)

  private def stopStreamingJob(indexName: String): Unit = {
    val job = spark.streams.active.find(_.name == indexName)
    if (job.isDefined) {
      job.get.stop()
    } else {
      logWarning("Refreshing job not found")
    }
  }

  /**
   * Transition the index state to FAILED upon encountering an exception. Retry in case conflicts
   * with final transaction in scheduled task.
   *
   * TODO: Determine the appropriate state code based on the type of exception encountered
   */
  private def retryUpdateIndexStateToFailed(
      indexName: String,
      exception: Option[Throwable]): Unit = {
    logInfo(s"Updating index state to failed for $indexName")
    retry {
      flintMetadataLogService
        .startTransaction(indexName)
        .initialLog(latest => latest.state == REFRESHING)
        .finalLog(latest =>
          exception match {
            case Some(ex) =>
              latest.copy(state = FAILED, error = extractRootCause(ex))
            case None =>
              latest.copy(state = FAILED)
          })
        .commit(_ => {})
    }
  }

  private def retry(operation: => Unit): Unit = {
    // Retry policy for 3 times every 1 second
    val retryPolicy = RetryPolicy
      .builder[Unit]()
      .handle(classOf[Throwable])
      .withBackoff(1, 30, SECONDS)
      .withJitter(Duration.ofMillis(100))
      .withMaxRetries(3)
      .onFailedAttempt((event: ExecutionAttemptedEvent[Unit]) =>
        logError("Attempt to update index state failed: " + event))
      .build()

    // Use the retry policy with Failsafe
    Failsafe
      .`with`(singletonList(retryPolicy))
      .run(new CheckedRunnable {
        override def run(): Unit = operation
      })
  }

  private def extractRootCause(e: Throwable): String = {
    var cause = e
    while (cause.getCause != null && cause.getCause != cause) {
      cause = cause.getCause
    }

    if (cause.getLocalizedMessage != null) {
      return cause.getLocalizedMessage
    }
    if (cause.getMessage != null) {
      return cause.getMessage
    }
    cause.toString
  }
}

object FlintSparkIndexMonitor extends Logging {

  /**
   * Thread-safe ExecutorService globally shared by all FlintSpark instance and will be shutdown
   * in Spark application upon exit. Non-final variable for test convenience.
   */
  var executor: ScheduledExecutorService =
    newDaemonThreadPoolScheduledExecutor("flint-index-heartbeat", 1)

  /**
   * Tracker that stores task future handle which is required to cancel the task in future.
   */
  val indexMonitorTracker: Map[String, ScheduledFuture[_]] =
    new TrieMap[String, ScheduledFuture[_]]()

  /*
   * Register shutdown hook to SparkContext with default priority (higher than SparkContext.close itself)
   */
  addShutdownHook(() => {
    logInfo("Shutdown scheduled executor service")
    try {
      executor.shutdownNow()
    } catch {
      case e: Exception => logWarning("Failed to shutdown scheduled executor service", e)
    }
  })
}
