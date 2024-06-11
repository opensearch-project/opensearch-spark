/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.action.get.{GetRequest, GetResponse}
import org.opensearch.client.RequestOptions
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.app.FlintInstance
import org.opensearch.flint.core.storage.OpenSearchUpdater
import org.scalatest.matchers.should.Matchers

class OpenSearchUpdaterSuite extends OpenSearchTransactionSuite with Matchers {
  val sessionId = "sessionId"
  val timestamp = 1700090926955L
  val flintJob =
    new FlintInstance(
      "applicationId",
      "jobId",
      sessionId,
      "running",
      timestamp,
      timestamp,
      Seq(""))
  var flintClient: FlintClient = _
  var updater: OpenSearchUpdater = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    flintClient = FlintClientBuilder.build(new FlintOptions(openSearchOptions.asJava));
    updater = new OpenSearchUpdater(
      testMetaLogIndex,
      FlintClientBuilder.build(new FlintOptions(openSearchOptions.asJava)))
  }

  test("upsert flintJob should success") {
    updater.upsert(sessionId, FlintInstance.serialize(flintJob, timestamp))
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe timestamp
  }

  test("index is deleted when upsert flintJob should throw IllegalStateException") {
    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.upsert(sessionId, FlintInstance.serialize(flintJob, timestamp))
    }
  }

  test("update flintJob should success") {
    updater.upsert(sessionId, FlintInstance.serialize(flintJob, timestamp))

    val newTimestamp = 1700090926956L
    updater.update(sessionId, FlintInstance.serialize(flintJob, newTimestamp))
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe newTimestamp
  }

  test("index is deleted when update flintJob should throw IllegalStateException") {
    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.update(sessionId, FlintInstance.serialize(flintJob, timestamp))
    }
  }

  test("updateIf flintJob should success") {
    updater.upsert(sessionId, FlintInstance.serialize(flintJob, timestamp))
    val (resp, latest) = getFlintInstance(sessionId)

    val newTimestamp = 1700090926956L
    updater.updateIf(
      sessionId,
      FlintInstance.serialize(latest, newTimestamp),
      resp.getSeqNo,
      resp.getPrimaryTerm)
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe newTimestamp
  }

  test("index is deleted when updateIf flintJob should throw IllegalStateException") {
    updater.upsert(sessionId, FlintInstance.serialize(flintJob, timestamp))
    val (resp, latest) = getFlintInstance(sessionId)

    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.updateIf(
        sessionId,
        FlintInstance.serialize(latest, timestamp),
        resp.getSeqNo,
        resp.getPrimaryTerm)
    }
  }

  def getFlintInstance(docId: String): (GetResponse, FlintInstance) = {
    val response =
      openSearchClient.get(new GetRequest(testMetaLogIndex, docId), RequestOptions.DEFAULT)
    (response, FlintInstance.deserializeFromMap(response.getSourceAsMap))
  }
}
