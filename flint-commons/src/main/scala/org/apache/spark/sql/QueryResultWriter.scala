/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintStatement

trait QueryResultWriter {
  def write(dataFrame: DataFrame, flintStatement: FlintStatement): Unit
}
