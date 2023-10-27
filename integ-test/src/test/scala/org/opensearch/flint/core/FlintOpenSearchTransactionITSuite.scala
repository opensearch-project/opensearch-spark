/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.{Base64, Collections}

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.FlintSparkSuite
import org.scalatest.matchers.should.Matchers

class FlintOpenSearchTransactionITSuite extends FlintSparkSuite with Matchers {

  val testFlintIndex = "flint_test_index"
  val testMetadataLogIndex = ".query_request_history_mys3"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  var flintClient: FlintClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    openSearchClient
      .indices()
      .create(new CreateIndexRequest(testMetadataLogIndex), RequestOptions.DEFAULT)
  }

  override def afterEach(): Unit = {
    openSearchClient
      .indices()
      .delete(new DeleteIndexRequest(testMetadataLogIndex), RequestOptions.DEFAULT)
    super.afterEach()
  }

  test("normal transition from initial to transient to final log") {
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

  test("should exit if initial log entry doesn't meet precondition") {
    the[IllegalStateException] thrownBy {
      flintClient
        .startTransaction(testFlintIndex)
        .initialLog(_ => false)
        .transientLog(latest => latest)
        .finalLog(latest => latest)
        .execute(() => {})
    }
  }

  private def latestLogEntry: Map[String, AnyRef] = {
    val response = openSearchClient
      .get(new GetRequest(testMetadataLogIndex, testLatestId), RequestOptions.DEFAULT)

    Option(response.getSourceAsMap).getOrElse(Collections.emptyMap()).asScala.toMap
  }
}
