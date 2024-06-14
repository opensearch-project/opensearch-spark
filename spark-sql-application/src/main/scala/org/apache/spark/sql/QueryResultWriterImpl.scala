/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintStatement

import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CleanerFactory

class QueryResultWriterImpl(context: Map[String, Any])
    extends QueryResultWriter
    with FlintJobExecutor
    with Logging {

  val resultIndex = context("resultIndex").asInstanceOf[String]
  val osClient = context("osClient").asInstanceOf[OSClient]

  override def reformatQueryResult(
      dataFrame: DataFrame,
      flintStatement: FlintStatement,
      queryExecutionContext: StatementExecutionContext): DataFrame = {
    import queryExecutionContext._
    getFormattedData(
      dataFrame,
      spark,
      dataSource,
      flintStatement.queryId,
      flintStatement.query,
      sessionId,
      flintStatement.queryStartTime.get,
      CleanerFactory.cleaner(false))
  }

  override def persistQueryResult(dataFrame: DataFrame, flintStatement: FlintStatement): Unit = {
    writeDataFrameToOpensearch(dataFrame, resultIndex, osClient)
  }
}
