/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.scheduler

import org.opensearch.flint.common.scheduler.AsyncQueryScheduler
import org.opensearch.flint.spark.{FlintSparkIndex, FlintSparkIndexMonitor}
import org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerBuilder.AsyncQuerySchedulerAction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.flint.config.FlintSparkConf

/**
 * Trait defining the interface for Flint Spark job scheduling services.
 */
trait FlintSparkJobSchedulingService {

  /**
   * Handles a job action for a given Flint Spark index.
   *
   * @param index
   *   The FlintSparkIndex to be processed
   * @param action
   *   The AsyncQuerySchedulerAction to be performed
   */
  def handleJob(index: FlintSparkIndex, action: AsyncQuerySchedulerAction): Unit

  /**
   * Checks if the external scheduler is enabled for a given Flint Spark index.
   *
   * @param index
   *   The FlintSparkIndex to check
   * @return
   *   true if external scheduler is enabled, false otherwise
   */
  def isExternalSchedulerEnabled(index: FlintSparkIndex): Boolean = {
    val autoRefresh = index.options.autoRefresh()
    val schedulerModeExternal = index.options.isExternalSchedulerEnabled()
    autoRefresh && schedulerModeExternal
  }
}

/**
 * Companion object for FlintSparkJobSchedulingService. Provides a factory method to create
 * appropriate scheduling service instances.
 */
object FlintSparkJobSchedulingService {

  /**
   * Creates a FlintSparkJobSchedulingService instance based on the index configuration.
   *
   * @param index
   *   The FlintSparkIndex for which the service is created
   * @param spark
   *   The SparkSession
   * @param flintAsyncQueryScheduler
   *   The AsyncQueryScheduler
   * @param flintSparkConf
   *   The FlintSparkConf configuration
   * @param flintIndexMonitor
   *   The FlintSparkIndexMonitor
   * @return
   *   An instance of FlintSparkJobSchedulingService
   */
  def create(
      index: FlintSparkIndex,
      spark: SparkSession,
      flintAsyncQueryScheduler: AsyncQueryScheduler,
      flintSparkConf: FlintSparkConf,
      flintIndexMonitor: FlintSparkIndexMonitor): FlintSparkJobSchedulingService = {
    if (index.options.isExternalSchedulerEnabled()) {
      new FlintSparkJobExternalSchedulingService(flintAsyncQueryScheduler, flintSparkConf)
    } else {
      new FlintSparkJobInternalSchedulingService(spark, flintIndexMonitor)
    }
  }
}
