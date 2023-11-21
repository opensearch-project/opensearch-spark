/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT}

class RetryableHttpAsyncClientITSuite extends AnyFlatSpec with OpenSearchSuite {

  lazy val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

  behavior of "Flint OpenSearch client"

  it should "not retry" in {
    val client = flintClient.createClient()
    val result = client.indices().exists(new GetIndexRequest("test"), RequestOptions.DEFAULT)
    result shouldBe false
    client.close()
  }

  it should "retry" in {
    val flintClient = new FlintOpenSearchClient(
      new FlintOptions(
        Map(
          s"${HOST_ENDPOINT.optionKey}" -> openSearchHost,
          s"${HOST_PORT.optionKey}" -> s"${openSearchPort + 1}").asJava))

    val client = flintClient.createClient()
    val result = client.indices().exists(new GetIndexRequest("test"), RequestOptions.DEFAULT)
    result shouldBe false
    client.close()
  }
}
