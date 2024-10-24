/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import java.util.{Base64, List}

import scala.collection.JavaConverters._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.common.FlintVersion.current
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService}
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

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(testFlintIndex)
    conf.unsetConf(FlintSparkConf.METADATA_CACHE_WRITE.key)
  }

  test("build disabled metadata cache writer") {
    FlintMetadataCacheWriterBuilder
      .build(FlintSparkConf()) shouldBe a[FlintDisabledMetadataCacheWriter]
  }

  test("build opensearch metadata cache writer") {
    setFlintSparkConf(FlintSparkConf.METADATA_CACHE_WRITE, "true")
    FlintMetadataCacheWriterBuilder
      .build(FlintSparkConf()) shouldBe a[FlintOpenSearchMetadataCacheWriter]
  }

  test("serialize metadata cache to JSON") {
    val expectedMetadataJson: String = s"""
      | {
      |   "_meta": {
      |     "version": "${current()}",
      |     "name": "${testFlintIndex}",
      |     "kind": "test_kind",
      |     "source": "test_source_table",
      |     "indexedColumns": [
      |     {
      |       "test_field": "spark_type"
      |     }],
      |     "options": {
      |       "auto_refresh": "true",
      |       "refresh_interval": "10 Minutes"
      |     },
      |     "properties": {
      |       "metadataCacheVersion": "1.0",
      |       "refreshInterval": 600,
      |       "sourceTables": ["${FlintMetadataCache.mockTableName}"],
      |       "lastRefreshTime": ${testLastRefreshCompleteTime}
      |     },
      |     "latestId": "${testLatestId}"
      |   },
      |   "properties": {
      |     "test_field": {
      |       "type": "os_type"
      |     }
      |   }
      | }
      |""".stripMargin
    val builder = new FlintMetadata.Builder
    builder.name(testFlintIndex)
    builder.kind("test_kind")
    builder.source("test_source_table")
    builder.addIndexedColumn(Map[String, AnyRef]("test_field" -> "spark_type").asJava)
    builder.options(
      Map("auto_refresh" -> "true", "refresh_interval" -> "10 Minutes")
        .mapValues(_.asInstanceOf[AnyRef])
        .asJava)
    builder.schema(Map[String, AnyRef]("test_field" -> Map("type" -> "os_type").asJava).asJava)
    builder.latestLogEntry(flintMetadataLogEntry)

    val metadata = builder.build()
    flintMetadataCacheWriter.serialize(metadata) should matchJson(expectedMetadataJson)
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
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)
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
    Entry("refreshInterval", 600),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)
  }

  test("exclude refresh interval in metadata cache when auto refresh is false") {
    val content =
      """ {
        |   "_meta": {
        |     "kind": "test_kind",
        |     "options": {
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
    properties should have size 3
    properties should contain allOf (Entry("metadataCacheVersion", "1.0"),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)
  }

  test("exclude last refresh time in metadata cache when index has not been refreshed") {
    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize("{}")
      .copy(latestLogEntry = Some(flintMetadataLogEntry.copy(lastRefreshCompleteTime = 0L)))
    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 2
    properties should contain(Entry("metadataCacheVersion", "1.0"))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)
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
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)

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
      .toArray should contain theSameElementsAs Array(FlintMetadataCache.mockTableName)
  }
}
