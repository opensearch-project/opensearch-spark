/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.benchmark

import org.apache.http.HttpHost
import org.opensearch.client.{RestClient, RestHighLevelClient}
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.testcontainers.OpenSearchContainer

import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.flint.config.FlintSparkConf._

/**
 * Flint benchmark base class for benchmarking with Spark and OpenSearch.
 * [[org.opensearch.flint.OpenSearchSuite]] doesn't work here because Spark's
 * [[SqlBasedBenchmark]] is not a Scala test suite.
 */
trait FlintSparkBenchmark extends SqlBasedBenchmark {

  protected lazy val flint: FlintSpark = new FlintSpark(spark)

  protected lazy val container = new OpenSearchContainer()

  protected lazy val openSearchPort: Int = container.port()

  protected lazy val openSearchHost: String = container.getHost

  protected lazy val openSearchClient = new RestHighLevelClient(
    RestClient.builder(new HttpHost(openSearchHost, openSearchPort, "http")))

  protected lazy val openSearchOptions: Map[String, String] =
    Map(
      s"${HOST_ENDPOINT.optionKey}" -> openSearchHost,
      s"${HOST_PORT.optionKey}" -> s"$openSearchPort",
      s"${REFRESH_POLICY.optionKey}" -> "wait_for",
      s"${IGNORE_DOC_ID_COLUMN.optionKey}" -> "false")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    container.start()
  }

  override def afterAll(): Unit = {
    container.close()
    super.afterAll()
  }
}
