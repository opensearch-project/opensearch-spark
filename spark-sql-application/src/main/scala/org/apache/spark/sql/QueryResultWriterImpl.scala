/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement

import org.apache.spark.internal.Logging
import org.apache.spark.sql.FlintJob.writeDataFrameToOpensearch
import org.apache.spark.sql.flint.config.FlintSparkConf

class QueryResultWriterImpl(context: Map[String, Any]) extends QueryResultWriter with Logging {

  private val resultIndex = context("resultIndex").asInstanceOf[String]
  // Initialize OSClient with Flint options because custom session manager implementation should not have it in the context
  private val osClient = new OSClient(FlintSparkConf().flintOptions())

  override def writeDataFrame(dataFrame: DataFrame, flintStatement: FlintStatement): Unit = {
    writeDataFrameToOpensearch(dataFrame, resultIndex, osClient)
  }
}
