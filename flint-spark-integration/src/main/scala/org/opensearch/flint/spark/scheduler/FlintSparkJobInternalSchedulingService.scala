/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState
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

  override val stateTransitions: StateTransitions = StateTransitions(
    initialStateForUpdate = IndexState.ACTIVE,
    finalStateForUpdate = IndexState.REFRESHING,
    initialStateForUnschedule = IndexState.REFRESHING,
    finalStateForUnschedule = IndexState.ACTIVE)

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
  override def handleJob(
      index: FlintSparkIndex,
      action: AsyncQuerySchedulerAction): Option[String] = {
    val indexName = index.name()
    action match {
      case AsyncQuerySchedulerAction.SCHEDULE => None // No-op
      case AsyncQuerySchedulerAction.UPDATE =>
        startRefreshingJob(index)
      case AsyncQuerySchedulerAction.UNSCHEDULE =>
        logInfo("Stopping index state monitor")
        flintIndexMonitor.stopMonitor(indexName)
        stopRefreshingJob(indexName)
        None
      case AsyncQuerySchedulerAction.REMOVE => None // No-op
      case _ => throw new IllegalArgumentException(s"Unsupported action: $action")
    }
  }

  /**
   * Starts a refreshing job for the given Flint Spark index.
   *
   * @param index
   *   The FlintSparkIndex for which to start the refreshing job
   */
  private def startRefreshingJob(index: FlintSparkIndex): Option[String] = {
    logInfo(s"Starting refreshing job for index ${index.name()}")
    val indexRefresh = FlintSparkIndexRefresh.create(index.name(), index)
    val jobId = indexRefresh.start(spark, new FlintSparkConf(spark.conf.getAll.toMap.asJava))

    // NOTE: Resolution for previous concurrency issue
    // This code addresses a previously identified concurrency issue with recoverIndex
    // where scheduled FlintSparkIndexMonitorTask couldn't detect the active Spark streaming job ID. The issue
    // was caused by starting the FlintSparkIndexMonitor before the Spark streaming job was fully
    // initialized. In this fixed version, we start the monitor after the streaming job has been
    // initiated, ensuring that the job ID is available for detection.
    logInfo("Scheduling index state monitor")
    flintIndexMonitor.startMonitor(index.name())
    jobId
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
