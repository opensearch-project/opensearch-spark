/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util.{Map => JMap}

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.mockito.Mockito.when
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import play.api.libs.json.{Json, JsValue}

class FlintMetadataLogEntryOpenSearchConverterTest
    extends AnyFlatSpec
    with BeforeAndAfter
    with Matchers {
  val mockLogEntry: FlintMetadataLogEntry = mock[FlintMetadataLogEntry]

  val sourceMap = JMap.of(
    "jobStartTime",
    1234567890123L.asInstanceOf[Object],
    "lastRefreshStartTime",
    1234567890123L.asInstanceOf[Object],
    "lastRefreshCompleteTime",
    1234567890123L.asInstanceOf[Object],
    "state",
    "active".asInstanceOf[Object],
    "dataSourceName",
    "testDataSource".asInstanceOf[Object],
    "error",
    "".asInstanceOf[Object])

  before {
    when(mockLogEntry.id).thenReturn("id")
    when(mockLogEntry.state).thenReturn(FlintMetadataLogEntry.IndexState.ACTIVE)
    when(mockLogEntry.createTime).thenReturn(1234567890123L)
    when(mockLogEntry.lastRefreshStartTime).thenReturn(1234567890123L)
    when(mockLogEntry.lastRefreshCompleteTime).thenReturn(1234567890123L)
    when(mockLogEntry.error).thenReturn("")
    when(mockLogEntry.properties).thenReturn(Map("dataSourceName" -> "testDataSource"))
  }

  it should "convert to json" in {
    // Removing lastUpdateTime since System.currentTimeMillis() cannot be mocked
    val expectedJsonWithoutLastUpdateTime =
      s"""
         |{
         |  "version": "1.1",
         |  "latestId": "id",
         |  "type": "flintindexstate",
         |  "state": "active",
         |  "applicationId": "unknown",
         |  "jobId": "unknown",
         |  "dataSourceName": "testDataSource",
         |  "jobStartTime": 1234567890123,
         |  "lastRefreshStartTime": 1234567890123,
         |  "lastRefreshCompleteTime": 1234567890123,
         |  "error": ""
         |}
         |""".stripMargin
    val actualJson = FlintMetadataLogEntryOpenSearchConverter.toJson(mockLogEntry)
    removeJsonField(actualJson, "lastUpdateTime") should matchJson(
      expectedJsonWithoutLastUpdateTime)
  }

  it should "construct log entry" in {
    val logEntry =
      FlintMetadataLogEntryOpenSearchConverter.constructLogEntry("id", 1L, 1L, sourceMap)
    logEntry shouldBe a[FlintMetadataLogEntry]
    logEntry.id shouldBe "id"
    logEntry.createTime shouldBe 1234567890123L
    logEntry.lastRefreshStartTime shouldBe 1234567890123L
    logEntry.lastRefreshCompleteTime shouldBe 1234567890123L
    logEntry.state shouldBe FlintMetadataLogEntry.IndexState.ACTIVE
    logEntry.error shouldBe ""
    logEntry.properties.get("dataSourceName").get shouldBe "testDataSource"
  }

  it should "construct log entry with integer timestamp value" in {
    // Use Integer instead of Long for timestamps
    val testSourceMap = JMap.of(
      "jobStartTime",
      1234567890.asInstanceOf[Object],
      "lastRefreshStartTime",
      1234567890.asInstanceOf[Object],
      "lastRefreshCompleteTime",
      1234567890.asInstanceOf[Object],
      "state",
      "active".asInstanceOf[Object],
      "dataSourceName",
      "testDataSource".asInstanceOf[Object],
      "error",
      "".asInstanceOf[Object])
    val logEntry =
      FlintMetadataLogEntryOpenSearchConverter.constructLogEntry("id", 1L, 1L, testSourceMap)
    logEntry shouldBe a[FlintMetadataLogEntry]
    logEntry.id shouldBe "id"
    logEntry.createTime shouldBe 1234567890
    logEntry.lastRefreshStartTime shouldBe 1234567890
    logEntry.lastRefreshCompleteTime shouldBe 1234567890
    logEntry.state shouldBe FlintMetadataLogEntry.IndexState.ACTIVE
    logEntry.error shouldBe ""
    logEntry.properties.get("dataSourceName").get shouldBe "testDataSource"
  }

  private def removeJsonField(json: String, field: String): String = {
    Json.stringify(Json.toJson(Json.parse(json).as[Map[String, JsValue]] - field))
  }
}
