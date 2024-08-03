/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintStatement

trait QueryResultWriter {
  def reformatQueryResult(
      dataFrame: DataFrame,
      flintStatement: FlintStatement,
      queryExecutionContext: StatementExecutionContext): DataFrame
  def persistQueryResult(dataFrame: DataFrame, flintStatement: FlintStatement): Unit
}
