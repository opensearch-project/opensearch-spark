/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

class FlintMetricSource(val sourceName: String) extends Source {
  override val metricRegistry: MetricRegistry = new MetricRegistry
}
