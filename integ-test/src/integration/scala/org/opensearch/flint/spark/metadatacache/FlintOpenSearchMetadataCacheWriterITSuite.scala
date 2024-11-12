/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import java.util.{Base64, List}

import scala.collection.JavaConverters._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.native.JsonMethods._
import org.opensearch.flint.common.FlintVersion.current
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService}
import org.opensearch.flint.spark.{FlintSparkIndexOptions, FlintSparkSuite}
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{getSkippingIndexName, SKIPPING_INDEX_TYPE}
import org.scalatest.Entry
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintOpenSearchMetadataCacheWriterITSuite extends FlintSparkSuite with Matchers {

  /** Lazy initialize after container started. */
  lazy val options = new FlintOptions(openSearchOptions.asJava)
  lazy val flintClient = new FlintOpenSearchClient(options)
  lazy val flintMetadataCacheWriter = new FlintOpenSearchMetadataCacheWriter(options)
  lazy val flintIndexMetadataService = new FlintOpenSearchIndexMetadataService(options)

  private val testTable = "spark_catalog.default.metadatacache_test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  private val testLastRefreshCompleteTime = 1234567890123L
  private val flintMetadataLogEntry = FlintMetadataLogEntry(
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
    createPartitionedMultiRowAddressTable(testTable)
  }

  override def afterAll(): Unit = {
    sql(s"DROP TABLE $testTable")
    super.afterAll()
  }

  override def afterEach(): Unit = {
    deleteTestIndex(testFlintIndex)
    super.afterEach()
  }

  test("build disabled metadata cache writer") {
    FlintMetadataCacheWriterBuilder
      .build(FlintSparkConf()) shouldBe a[FlintDisabledMetadataCacheWriter]
  }

  test("build opensearch metadata cache writer") {
    setFlintSparkConf(FlintSparkConf.METADATA_CACHE_WRITE, "true")
    withMetadataCacheWriteEnabled {
      FlintMetadataCacheWriterBuilder
        .build(FlintSparkConf()) shouldBe a[FlintOpenSearchMetadataCacheWriter]
    }
  }

  test("serialize metadata cache to JSON") {
    val expectedMetadataJson: String = s"""
      | {
      |   "_meta": {
      |     "version": "${current()}",
      |     "name": "$testFlintIndex",
      |     "kind": "test_kind",
      |     "source": "$testTable",
      |     "indexedColumns": [
      |     {
      |       "test_field": "spark_type"
      |     }],
      |     "options": {
      |       "auto_refresh": "true",
      |       "refresh_interval": "10 Minutes"
      |     },
      |     "properties": {
      |       "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
      |       "refreshInterval": 600,
      |       "sourceTables": ["$testTable"],
      |       "lastRefreshTime": $testLastRefreshCompleteTime
      |     },
      |     "latestId": "$testLatestId"
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
    builder.source(testTable)
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
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
  }

  Seq(SKIPPING_INDEX_TYPE, COVERING_INDEX_TYPE).foreach { case kind =>
    test(s"write metadata cache to $kind index mappings with source tables") {
      val content =
        s""" {
           |   "_meta": {
           |     "kind": "$kind",
           |     "source": "$testTable"
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
      properties
        .get("sourceTables")
        .asInstanceOf[java.util.ArrayList[String]] should contain theSameElementsAs Array(
        testTable)
    }
  }

  test("write metadata cache with source tables from index metadata") {
    val mv = FlintSparkMaterializedView(
      "spark_catalog.default.mv",
      s"SELECT 1 FROM $testTable",
      Array(testTable),
      Map("1" -> "integer"))
    val metadata = mv.metadata().copy(latestLogEntry = Some(flintMetadataLogEntry))

    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties
      .get("sourceTables")
      .asInstanceOf[java.util.ArrayList[String]] should contain theSameElementsAs Array(testTable)
  }

  test("write metadata cache with source tables from deserialized metadata") {
    val testTable2 = "spark_catalog.default.metadatacache_test2"
    val content =
      s""" {
         |   "_meta": {
         |     "kind": "$MV_INDEX_TYPE",
         |     "properties": {
         |       "sourceTables": [
         |         "$testTable", "$testTable2"
         |       ]
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
    properties
      .get("sourceTables")
      .asInstanceOf[java.util.ArrayList[String]] should contain theSameElementsAs Array(
      testTable,
      testTable2)
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
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("refreshInterval", 600),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
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
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
  }

  test("exclude last refresh time in metadata cache when index has not been refreshed") {
    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize("{}")
      .copy(latestLogEntry = Some(flintMetadataLogEntry.copy(lastRefreshCompleteTime = 0L)))
    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 2
    properties should contain(
      Entry("metadataCacheVersion", FlintMetadataCache.metadataCacheVersion))
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
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))

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
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("lastRefreshTime", testLastRefreshCompleteTime))
  }

  Seq(
    (
      "auto refresh index with external scheduler",
      Map(
        "auto_refresh" -> "true",
        "scheduler_mode" -> "external",
        "refresh_interval" -> "10 Minute",
        "checkpoint_location" -> "s3a://test/"),
      s"""
         | {
         |   "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
         |   "refreshInterval": 600,
         |   "sourceTables": ["$testTable"]
         | }
         |""".stripMargin),
    (
      "full refresh index",
      Map.empty[String, String],
      s"""
         | {
         |   "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
         |   "sourceTables": ["$testTable"]
         | }
         |""".stripMargin),
    (
      "incremental refresh index",
      Map("incremental_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
      s"""
         | {
         |   "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
         |   "sourceTables": ["$testTable"]
         | }
         |""".stripMargin)).foreach { case (refreshMode, optionsMap, expectedJson) =>
    test(s"write metadata cache for $refreshMode") {
      withExternalSchedulerEnabled {
        withMetadataCacheWriteEnabled {
          withTempDir { checkpointDir =>
            // update checkpoint_location if available in optionsMap
            val indexOptions = FlintSparkIndexOptions(
              optionsMap
                .get("checkpoint_location")
                .map(_ =>
                  optionsMap.updated("checkpoint_location", checkpointDir.getAbsolutePath))
                .getOrElse(optionsMap))

            flint
              .skippingIndex()
              .onTable(testTable)
              .addMinMax("age")
              .options(indexOptions, testFlintIndex)
              .create()

            var index = flint.describeIndex(testFlintIndex)
            index shouldBe defined
            val propertiesJson =
              compact(
                render(
                  parse(
                    flintMetadataCacheWriter.serialize(
                      index.get.metadata())) \ "_meta" \ "properties"))
            propertiesJson should matchJson(expectedJson)

            flint.refreshIndex(testFlintIndex)
            index = flint.describeIndex(testFlintIndex)
            index shouldBe defined
            val lastRefreshTime =
              compact(
                render(
                  parse(
                    flintMetadataCacheWriter.serialize(
                      index.get.metadata())) \ "_meta" \ "properties" \ "lastRefreshTime")).toLong
            lastRefreshTime should be > 0L
          }
        }
      }
    }
  }

  test("write metadata cache for auto refresh index with internal scheduler") {
    withMetadataCacheWriteEnabled {
      withTempDir { checkpointDir =>
        flint
          .skippingIndex()
          .onTable(testTable)
          .addMinMax("age")
          .options(
            FlintSparkIndexOptions(
              Map(
                "auto_refresh" -> "true",
                "scheduler_mode" -> "internal",
                "refresh_interval" -> "10 Minute",
                "checkpoint_location" -> checkpointDir.getAbsolutePath)),
            testFlintIndex)
          .create()

        var index = flint.describeIndex(testFlintIndex)
        index shouldBe defined
        val propertiesJson =
          compact(
            render(parse(
              flintMetadataCacheWriter.serialize(index.get.metadata())) \ "_meta" \ "properties"))
        propertiesJson should matchJson(s"""
            | {
            |   "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
            |   "refreshInterval": 600,
            |   "sourceTables": ["$testTable"]
            | }
            |""".stripMargin)

        flint.refreshIndex(testFlintIndex)
        index = flint.describeIndex(testFlintIndex)
        index shouldBe defined
        compact(render(parse(
          flintMetadataCacheWriter.serialize(
            index.get.metadata())) \ "_meta" \ "properties")) should not include "lastRefreshTime"
      }
    }
  }
}
