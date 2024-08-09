/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

trait QueryResultWriter {
  def writeDataFrame(dataFrame: DataFrame, flintStatement: FlintStatement): Unit
}
