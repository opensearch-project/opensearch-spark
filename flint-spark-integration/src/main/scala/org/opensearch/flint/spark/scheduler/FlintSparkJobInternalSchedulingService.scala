/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.spark.{FlintSparkIndex, FlintSparkIndexMonitor}
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder.AsyncQuerySchedulerAction

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Internal scheduling service for Flint Spark jobs.
 *
 * This class implements the FlintSparkJobSchedulingService interface and provides functionality
 * to handle job scheduling, updating, and unscheduling using internal Spark mechanisms and
 * FlintSparkIndexMonitor.
 *
 * @param spark
 *   The SparkSession
 * @param flintIndexMonitor
 *   The FlintSparkIndexMonitor used for index monitoring
 */
class FlintSparkJobInternalSchedulingService(
    spark: SparkSession,
    flintIndexMonitor: FlintSparkIndexMonitor)
    extends FlintSparkJobSchedulingService
    with Logging {

  /**
   * Handles job-related actions for a given Flint Spark index.
   *
   * This method processes different actions (schedule, update, unschedule) for a Flint Spark
   * index using internal scheduling mechanisms.
   *
   * @param index
   *   The FlintSparkIndex to be processed
   * @param action
   *   The AsyncQuerySchedulerAction to be performed
   */
  override def handleJob(index: FlintSparkIndex, action: AsyncQuerySchedulerAction): Unit = {
    val indexName = index.name()

    action match {
      case AsyncQuerySchedulerAction.SCHEDULE =>
        logInfo("Scheduling index state monitor")
        flintIndexMonitor.startMonitor(indexName)
      case AsyncQuerySchedulerAction.UPDATE =>
        logInfo("Updating index state monitor")
        flintIndexMonitor.startMonitor(indexName)
        startRefreshingJob(index)
      case AsyncQuerySchedulerAction.UNSCHEDULE =>
        logInfo("Stopping index state monitor")
        flintIndexMonitor.stopMonitor(indexName)
        stopRefreshingJob(indexName)
      case AsyncQuerySchedulerAction.REMOVE => // No-op
      case _ => throw new IllegalArgumentException(s"Unsupported action: $action")
    }
  }

  /**
   * Starts a refreshing job for the given Flint Spark index.
   *
   * @param index
   *   The FlintSparkIndex for which to start the refreshing job
   */
  private def startRefreshingJob(index: FlintSparkIndex): Unit = {
    logInfo(s"Starting refreshing job for index ${index.name()}")
    val indexRefresh = FlintSparkIndexRefresh.create(index.name(), index)
    indexRefresh.start(spark, new FlintSparkConf(spark.conf.getAll.toMap.asJava))
  }

  /**
   * Stops the refreshing job for the given index name.
   *
   * @param indexName
   *   The name of the index for which to stop the refreshing job
   */
  private def stopRefreshingJob(indexName: String): Unit = {
    logInfo(s"Terminating refreshing job $indexName")
    val job = spark.streams.active.find(_.name == indexName)
    if (job.isDefined) {
      job.get.stop()
    } else {
      logWarning("Refreshing job not found")
    }
  }
}
