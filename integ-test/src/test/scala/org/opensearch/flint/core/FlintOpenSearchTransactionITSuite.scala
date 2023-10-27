/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.{Base64, Collections}

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.FlintSparkSuite
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchTransactionITSuite extends FlintSparkSuite with Matchers {

  val testFlintIndex = "flint_test_index"
  val testMetadataLogIndex = ".query_request_history_mys3"
  val testLatestId = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    openSearchClient
      .indices()
      .create(new CreateIndexRequest(testMetadataLogIndex), RequestOptions.DEFAULT)
  }

  test("test") {
    val flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

    flintClient
      .startTransaction(testFlintIndex)
      .initialLog(latest => {
        latest.state shouldBe "empty"
        true
      })
      .transientLog(latest => latest.copy(state = "creating"))
      .finalLog(latest => latest.copy(state = "created"))
      .execute(() => {
        latestLogEntry should contain("state" -> "creating")
        null
      })

    latestLogEntry should contain("state" -> "created")
  }

  private def latestLogEntry: Map[String, AnyRef] = {
    val response = openSearchClient
      .get(new GetRequest(testMetadataLogIndex, testLatestId), RequestOptions.DEFAULT)

    Option(response.getSourceAsMap).getOrElse(Collections.emptyMap()).asScala.toMap
  }
}
