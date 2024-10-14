/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh.util

import org.opensearch.flint.core.metrics.MetricsUtil

/**
 * A trait that provides aspect-oriented metrics functionality for refresh operations.
 *
 * This trait can be mixed into classes that need to track metrics for various operations,
 * particularly those related to index refreshing. It provides a method to wrap operations with
 * metric collection, including timing and success/failure counting.
 */
trait RefreshMetricsAspect {

  /**
   * Wraps an operation with metric collection.
   *
   * @param clientId
   *   The ID of the client performing the operation
   * @param dataSource
   *   The name of the data source being used
   * @param indexName
   *   The name of the index being operated on
   * @param metricPrefix
   *   The prefix for the metrics (e.g., "incrementalRefresh")
   * @param block
   *   The operation to be performed and measured
   * @return
   *   The result of the operation
   *
   * This method will:
   *   1. Start a timer for the operation 2. Execute the provided operation 3. Increment a success
   *      or failure counter based on the outcome 4. Stop the timer 5. Return the result of the
   *      operation or throw any exception that occurred
   */
  def withMetrics(clientId: String, dataSource: String, indexName: String, metricPrefix: String)(
      block: => Option[String]): Option[String] = {
    val refreshMetricsHelper = new RefreshMetricsHelper(clientId, dataSource, indexName)

    val processingTimeMetric = s"$metricPrefix.processingTime"
    val successMetric = s"$metricPrefix.success.count"
    val failedMetric = s"$metricPrefix.failed.count"

    val timerContext = refreshMetricsHelper.getTimerContext(processingTimeMetric)

    try {
      val result = block
      refreshMetricsHelper.incrementCounter(successMetric)
      result
    } catch {
      case e: Exception =>
        refreshMetricsHelper.incrementCounter(failedMetric)
        throw e
    } finally {
      MetricsUtil.stopTimer(timerContext)
    }
  }
}
