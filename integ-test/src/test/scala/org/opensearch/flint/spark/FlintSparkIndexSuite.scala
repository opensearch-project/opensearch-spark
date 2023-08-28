/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.OpenSearchSuite

import org.apache.spark.FlintSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY}
import org.apache.spark.sql.streaming.StreamTest

trait FlintSparkIndexSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with StreamTest {

  /** Flint Spark high level API being tested */
  lazy protected val flint: FlintSpark = {
    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, "true")
    new FlintSpark(spark)
  }
}
