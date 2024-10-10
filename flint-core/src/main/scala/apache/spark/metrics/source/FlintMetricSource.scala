/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

/**
 * Metric source for general Flint metrics.
 */
class FlintMetricSource extends Source {

  // Implementing the Source trait
  override val sourceName: String = FlintMetricSource.FLINT_METRIC_SOURCE_NAME
  override val metricRegistry: MetricRegistry = new MetricRegistry
}

/**
 * Metric source for Flint index-specific metrics.
 */
class FlintIndexMetricSource extends Source {
  override val sourceName: String = FlintMetricSource.FLINT_INDEX_METRIC_SOURCE_NAME
  override val metricRegistry: MetricRegistry = new MetricRegistry
}

object FlintMetricSource {
  val FLINT_METRIC_SOURCE_NAME = "Flint" // Default source name
  val FLINT_INDEX_METRIC_SOURCE_NAME = "FlintIndex" // Index specific source name
}
