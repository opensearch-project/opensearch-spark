/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.concurrent.{Map, TrieMap}
import scala.sys.addShutdownHook

import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.{FAILED, REFRESHING}
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.newDaemonThreadPoolScheduledExecutor

/**
 * Flint Spark index state monitor.
 *
 * @param spark
 *   Spark session
 * @param flintClient
 *   Flint client
 * @param dataSourceName
 *   data source name
 */
class FlintSparkIndexMonitor(
    spark: SparkSession,
    flintClient: FlintClient,
    dataSourceName: String)
    extends Logging {

  /**
   * Start monitoring task on the given Flint index.
   *
   * @param indexName
   *   Flint index name
   */
  def startMonitor(indexName: String): Unit = {
    val task = FlintSparkIndexMonitor.executor.scheduleWithFixedDelay(
      new FlintSparkIndexMonitorTask(indexName),
      15, // Delay to ensure final logging is complete first, otherwise version conflicts
      60, // TODO: make interval configurable
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
    val task = FlintSparkIndexMonitor.indexMonitorTracker.remove(indexName)
    if (task.isDefined) {
      task.get.cancel(true)
    } else {
      logInfo(s"Cannot find scheduled task")
    }
  }

  private class FlintSparkIndexMonitorTask(indexName: String) extends Runnable {

    /** Error counter */
    private var errorCnt = 0

    override def run(): Unit = {
      logInfo(s"Scheduler trigger index monitor task for $indexName")
      try {
        if (isStreamingJobActive(indexName)) {
          logInfo("Streaming job is still active")
          flintClient
            .startTransaction(indexName, dataSourceName)
            .initialLog(latest => latest.state == REFRESHING)
            .finalLog(latest => latest) // timestamp will update automatically
            .commit(_ => {})
        } else {
          logError("Streaming job is not active. Cancelling monitor task")
          flintClient
            .startTransaction(indexName, dataSourceName)
            .initialLog(_ => true)
            .finalLog(latest => latest.copy(state = FAILED))
            .commit(_ => {})

          stopMonitor(indexName)
          logInfo("Index monitor task is cancelled")
        }

        // Reset counter if success
        errorCnt = 0
      } catch {
        case e: Throwable =>
          errorCnt += 1
          logError(s"Failed to update index log entry, consecutive errors: $errorCnt", e)
          MetricsUtil.incrementCounter(MetricConstants.STREAMING_HEARTBEAT_FAILED_METRIC)

          // Stop streaming job and its monitor if max retry limit reached
          if (errorCnt >= 10) {
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
