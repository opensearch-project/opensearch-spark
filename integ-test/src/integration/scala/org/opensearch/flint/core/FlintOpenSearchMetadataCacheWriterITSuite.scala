/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.{Base64, List}

import scala.collection.JavaConverters._

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService, FlintOpenSearchMetadataCacheWriter}
import org.opensearch.flint.spark.FlintSparkSuite
import org.scalatest.Entry
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintOpenSearchMetadataCacheWriterITSuite extends FlintSparkSuite with Matchers {

  /** Lazy initialize after container started. */
  lazy val options = new FlintOptions(openSearchOptions.asJava)
  lazy val flintClient = new FlintOpenSearchClient(options)
  lazy val flintMetadataCacheWriter = new FlintOpenSearchMetadataCacheWriter(options)
  lazy val flintIndexMetadataService = new FlintOpenSearchIndexMetadataService(options)

  val testFlintIndex = "flint_metadata_cache"
  val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  val testLastRefreshCompleteTime = 1234567890123L
  val flintMetadataLogEntry = FlintMetadataLogEntry(
    testLatestId,
    0L,
    0L,
    testLastRefreshCompleteTime,
    FlintMetadataLogEntry.IndexState.ACTIVE,
    Map.empty[String, Any],
    "",
    Map.empty[String, Any])

  override def beforeAll(): Unit = {
    super.beforeAll()
    setFlintSparkConf(FlintSparkConf.METADATA_CACHE_WRITE, "true")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // TODO: unset if default is false
    // conf.unsetConf(FlintSparkConf.METADATA_CACHE_WRITE.key)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(testFlintIndex)
  }

  test("write metadata cache to index mappings") {
    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize("{}")
      .copy(latestLogEntry = Some(flintMetadataLogEntry))
    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 3
    properties should contain allOf (Entry("metadataCacheVersion", "1.0"),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(
      "mock.mock.mock"
    ) // TODO: value from FlintMetadataCache mock constant
  }

  test("write metadata cache to index mappings with refresh interval") {
    val content =
      """ {
        |   "_meta": {
        |     "kind": "test_kind",
        |     "options": {
        |       "auto_refresh": "true",
        |       "refresh_interval": "10 Minutes"
        |     }
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin
    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize(content)
      .copy(latestLogEntry = Some(flintMetadataLogEntry))
    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 4
    properties should contain allOf (Entry("metadataCacheVersion", "1.0"),
    Entry("refreshInterval", 900), // TODO: value from FlintMetadataCache mock constant
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(
      "mock.mock.mock"
    ) // TODO: value from FlintmetadataCache mock constant
  }

  test("write metadata cache to index mappings and preserve other index metadata") {
    val content =
      """ {
        |   "_meta": {
        |     "kind": "test_kind"
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin

    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize(content)
      .copy(latestLogEntry = Some(flintMetadataLogEntry))
    flintClient.createIndex(testFlintIndex, metadata)

    flintIndexMetadataService.updateIndexMetadata(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    flintIndexMetadataService.getIndexMetadata(testFlintIndex).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).name shouldBe empty
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).schema should have size 1
    var properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 3
    properties should contain allOf (Entry("metadataCacheVersion", "1.0"),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(
      "mock.mock.mock"
    ) // TODO: value from FlintMetadataCache mock constant

    val newContent =
      """ {
        |   "_meta": {
        |     "kind": "test_kind",
        |     "name": "test_name"
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin

    val newMetadata = FlintOpenSearchIndexMetadataService
      .deserialize(newContent)
      .copy(latestLogEntry = Some(flintMetadataLogEntry))
    flintIndexMetadataService.updateIndexMetadata(testFlintIndex, newMetadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, newMetadata)

    flintIndexMetadataService.getIndexMetadata(testFlintIndex).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).name shouldBe "test_name"
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).schema should have size 1
    properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 3
    properties should contain allOf (Entry("metadataCacheVersion", "1.0"),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(
      "mock.mock.mock"
    ) // TODO: value from FlintMetadataCache mock constant
  }
}
