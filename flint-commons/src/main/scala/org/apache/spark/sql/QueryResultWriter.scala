/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

/**
 * Trait for writing the result of a query execution to an external data storage.
 */
trait QueryResultWriter {

  /**
   * Writes the given DataFrame, which represents the result of a query execution, to an external
   * data storage based on the provided FlintStatement metadata.
   */
  def writeDataFrame(dataFrame: DataFrame, flintStatement: FlintStatement): Unit
}
