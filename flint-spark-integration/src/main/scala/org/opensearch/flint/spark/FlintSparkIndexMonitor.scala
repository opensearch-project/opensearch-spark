/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.concurrent.{Map, TrieMap}
import scala.sys.addShutdownHook

import org.opensearch.flint.core.FlintClient
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.{FAILED, REFRESHING}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

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
      () => {
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
        } catch {
          case e: Exception =>
            logError("Failed to update index log entry", e)
            throw new IllegalStateException("Failed to update index log entry")
        }
      },
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

  private def isStreamingJobActive(indexName: String): Boolean =
    spark.streams.active.exists(_.name == indexName)
}

object FlintSparkIndexMonitor extends Logging {

  /**
   * Thread-safe ExecutorService globally shared by all FlintSpark instance and will be shutdown
   * in Spark application upon exit. Non-final variable for test convenience.
   */
  var executor: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

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
