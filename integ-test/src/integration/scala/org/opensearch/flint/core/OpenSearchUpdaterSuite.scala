/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.action.get.{GetRequest, GetResponse}
import org.opensearch.client.RequestOptions
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.common.model.InteractiveSession
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, OpenSearchUpdater}
import org.scalatest.matchers.should.Matchers

class OpenSearchUpdaterSuite extends OpenSearchTransactionSuite with Matchers {
  val sessionId = "sessionId"
  val timestamp = 1700090926955L
  val flintJob =
    new InteractiveSession(
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
    flintClient = new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava));
    updater = new OpenSearchUpdater(
      testMetaLogIndex,
      new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava)))
  }

  test("upsert flintJob should success") {
    updater.upsert(sessionId, InteractiveSession.serialize(flintJob, timestamp))
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe timestamp
  }

  test("index is deleted when upsert flintJob should throw IllegalStateException") {
    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.upsert(sessionId, InteractiveSession.serialize(flintJob, timestamp))
    }
  }

  test("update flintJob should success") {
    updater.upsert(sessionId, InteractiveSession.serialize(flintJob, timestamp))

    val newTimestamp = 1700090926956L
    updater.update(sessionId, InteractiveSession.serialize(flintJob, newTimestamp))
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe newTimestamp
  }

  test("index is deleted when update flintJob should throw IllegalStateException") {
    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.update(sessionId, InteractiveSession.serialize(flintJob, timestamp))
    }
  }

  test("updateIf flintJob should success") {
    updater.upsert(sessionId, InteractiveSession.serialize(flintJob, timestamp))
    val (resp, latest) = getFlintInstance(sessionId)

    val newTimestamp = 1700090926956L
    updater.updateIf(
      sessionId,
      InteractiveSession.serialize(latest, newTimestamp),
      resp.getSeqNo,
      resp.getPrimaryTerm)
    getFlintInstance(sessionId)._2.lastUpdateTime shouldBe newTimestamp
  }

  test("index is deleted when updateIf flintJob should throw IllegalStateException") {
    updater.upsert(sessionId, InteractiveSession.serialize(flintJob, timestamp))
    val (resp, latest) = getFlintInstance(sessionId)

    deleteIndex(testMetaLogIndex)

    the[IllegalStateException] thrownBy {
      updater.updateIf(
        sessionId,
        InteractiveSession.serialize(latest, timestamp),
        resp.getSeqNo,
        resp.getPrimaryTerm)
    }
  }

  def getFlintInstance(docId: String): (GetResponse, InteractiveSession) = {
    val response =
      openSearchClient.get(new GetRequest(testMetaLogIndex, docId), RequestOptions.DEFAULT)
    (response, InteractiveSession.deserializeFromMap(response.getSourceAsMap))
  }
}
