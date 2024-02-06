/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

class FlintMetricSource() extends Source {

  // Implementing the Source trait
  override val sourceName: String = FlintMetricSource.FLINT_METRIC_SOURCE_NAME
  override val metricRegistry: MetricRegistry = new MetricRegistry
}

object FlintMetricSource {
  val FLINT_METRIC_SOURCE_NAME = "Flint" // Default source name
}
