/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.sql

import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}

trait IndexMetricHelper {
  def emitCreateIndexMetric(autoRefresh: Boolean): Unit = {
    MetricsUtil.incrementCounter(MetricConstants.QUERY_CREATE_INDEX_COUNT_METRIC)
    if (autoRefresh) {
      MetricsUtil.incrementCounter(MetricConstants.QUERY_CREATE_INDEX_AUTO_REFRESH_COUNT_METRIC)
    } else {
      MetricsUtil.incrementCounter(MetricConstants.QUERY_CREATE_INDEX_MANUAL_REFRESH_COUNT_METRIC)
    }
  }

  def emitRefreshIndexMetric(): Unit = {
    MetricsUtil.incrementCounter(MetricConstants.QUERY_REFRESH_COUNT_METRIC)
  }

  def emitAlterIndexMetric(): Unit = {
    MetricsUtil.incrementCounter(MetricConstants.QUERY_ALTER_COUNT_METRIC)
  }

  def emitDropIndexMetric(): Unit = {
    MetricsUtil.incrementCounter(MetricConstants.QUERY_DROP_COUNT_METRIC)
  }

  def emitVacuumIndexMetric(): Unit = {
    MetricsUtil.incrementCounter(MetricConstants.QUERY_VACUUM_COUNT_METRIC)
  }
}
