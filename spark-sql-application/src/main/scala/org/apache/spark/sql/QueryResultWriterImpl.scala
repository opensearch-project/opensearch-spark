/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.writeDataFrameToOpensearch

class QueryResultWriterImpl(context: Map[String, Any]) extends QueryResultWriter with Logging {

  private val resultIndex = context("resultIndex").asInstanceOf[String]
  private val osClient = context("osClient").asInstanceOf[OSClient]

  override def writeDataFrame(dataFrame: DataFrame, flintStatement: FlintStatement): Unit = {
    writeDataFrameToOpensearch(dataFrame, resultIndex, osClient)
  }
}
