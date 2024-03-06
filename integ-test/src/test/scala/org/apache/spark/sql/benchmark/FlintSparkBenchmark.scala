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

trait FlintSparkBenchmark extends SqlBasedBenchmark {

  protected lazy val flint: FlintSpark = new FlintSpark(spark)

  protected lazy val container = new OpenSearchContainer()

  protected lazy val openSearchPort: Int = container.port()

  protected lazy val openSearchHost: String = container.getHost

  protected lazy val openSearchClient = new RestHighLevelClient(
    RestClient.builder(new HttpHost(openSearchHost, openSearchPort, "http")))

  def beforeAll(): Unit = {
    container.start()

    spark.conf.set(HOST_ENDPOINT.key, openSearchHost)
    spark.conf.set(HOST_PORT.key, openSearchPort)
    spark.conf.set(REFRESH_POLICY.key, "true")
    spark.conf.set(CHECKPOINT_MANDATORY.key, "false")
  }

  override def afterAll(): Unit = {
    container.close()
    super.afterAll()
  }
}
