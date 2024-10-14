/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.refresh.util

import com.codahale.metrics.Timer
import org.opensearch.flint.core.metrics.MetricsUtil

/**
 * Helper class for constructing dimensioned metric names used in refresh operations.
 */
class RefreshMetricsHelper(clientId: String, dataSource: String, indexName: String) {
  private val isIndexMetric = true

  /**
   * Increments a counter metric with the specified dimensioned name.
   *
   * @param metricName
   *   The name of the metric to increment
   */
  def incrementCounter(metricName: String): Unit = {
    MetricsUtil.incrementCounter(
      RefreshMetricsHelper.constructDimensionedMetricName(
        metricName,
        clientId,
        dataSource,
        indexName),
      isIndexMetric)
  }

  /**
   * Gets a timer context for the specified metric name.
   *
   * @param metricName
   *   The name of the metric
   * @return
   *   A Timer.Context object
   */
  def getTimerContext(metricName: String): Timer.Context = {
    MetricsUtil.getTimerContext(
      RefreshMetricsHelper.constructDimensionedMetricName(
        metricName,
        clientId,
        dataSource,
        indexName),
      isIndexMetric)
  }
}

object RefreshMetricsHelper {

  /**
   * Constructs a dimensioned metric name for external scheduler request count.
   *
   * @param metricName
   *   The name of the metric
   * @param clientId
   *   The ID of the client making the request
   * @param dataSource
   *   The data source being used
   * @param indexName
   *   The name of the index being refreshed
   * @return
   *   A formatted string representing the dimensioned metric name
   */
  private def constructDimensionedMetricName(
      metricName: String,
      clientId: String,
      dataSource: String,
      indexName: String): String = {
    s"${metricName}[clientId##${clientId},dataSource##${dataSource},indexName##${indexName}]"
  }
}
