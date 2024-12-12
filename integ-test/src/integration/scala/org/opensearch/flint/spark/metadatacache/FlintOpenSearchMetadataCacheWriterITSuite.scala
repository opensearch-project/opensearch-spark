/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import java.util.{Base64, Map => JMap}

import scala.collection.JavaConverters._

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.native.JsonMethods._
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchClient, FlintOpenSearchIndexMetadataService, OpenSearchClientUtils}
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndexOptions, FlintSparkSuite}
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
    withMetadataCacheWriteEnabled {
      FlintMetadataCacheWriterBuilder
        .build(FlintSparkConf()) shouldBe a[FlintOpenSearchMetadataCacheWriter]
    }
  }

  test("write metadata cache to index mappings") {
    val content =
      s""" {
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

  Seq(SKIPPING_INDEX_TYPE, COVERING_INDEX_TYPE).foreach { case kind =>
    test(s"write metadata cache to $kind index mappings with source tables for non mv index") {
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
      properties should not contain key("sourceQuery")
    }
  }

  test("write metadata cache with source tables and query from mv index metadata") {
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
    properties
      .get("sourceQuery")
      .asInstanceOf[String] shouldBe s"SELECT 1 FROM $testTable"
  }

  test("write metadata cache with source tables and query from deserialized mv metadata") {
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
    properties should not contain key("refreshInterval")
  }

  test("exclude last refresh time in metadata cache when index has not been refreshed") {
    val content =
      s""" {
         |   "properties": {
         |     "age": {
         |       "type": "integer"
         |     }
         |   }
         | }
         |""".stripMargin
    val metadata = FlintOpenSearchIndexMetadataService
      .deserialize(content)
      .copy(latestLogEntry = Some(flintMetadataLogEntry.copy(lastRefreshCompleteTime = 0L)))
    flintClient.createIndex(testFlintIndex, metadata)
    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should not contain key("lastRefreshTime")
  }

  test("write metadata cache to index mappings and preserve other index metadata") {
    val content =
      """ {
        |   "_meta": {
        |     "kind": "test_kind",
        |     "name": "test_name",
        |     "custom": "test_custom",
        |     "properties": {
        |       "custom_in_properties": "test_custom"
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

    // Simulates index mapping updated by custom implementation of FlintIndexMetadataService
    // with the extra "custom" field.
    val client = OpenSearchClientUtils.createClient(options)
    flintMetadataCacheWriter.updateIndexMapping(client, testFlintIndex, content)

    flintMetadataCacheWriter.updateMetadataCache(testFlintIndex, metadata)

    flintIndexMetadataService.getIndexMetadata(testFlintIndex).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).name shouldBe "test_name"
    flintIndexMetadataService.getIndexMetadata(testFlintIndex).schema should have size 1
    val properties = flintIndexMetadataService.getIndexMetadata(testFlintIndex).properties
    properties should have size 4
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      FlintMetadataCache.metadataCacheVersion),
    Entry("lastRefreshTime", testLastRefreshCompleteTime), Entry(
      "custom_in_properties",
      "test_custom"))

    // Directly get the index mapping and verify custom field is preserved
    flintMetadataCacheWriter
      .getIndexMapping(client, testFlintIndex)
      .get("_meta")
      .asInstanceOf[JMap[String, AnyRef]]
      .get("custom") shouldBe "test_custom"
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
          val flint: FlintSpark = new FlintSpark(spark)
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

            val propertiesJson = compact(render(getPropertiesJValue(testFlintIndex)))
            propertiesJson should matchJson(expectedJson)

            flint.refreshIndex(testFlintIndex)
            val lastRefreshTime =
              compact(render(getPropertiesJValue(testFlintIndex) \ "lastRefreshTime")).toLong
            lastRefreshTime should be > 0L
          }
        }
      }
    }
  }

  test("write metadata cache for auto refresh index with internal scheduler") {
    withMetadataCacheWriteEnabled {
      val flint: FlintSpark = new FlintSpark(spark)
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

        val propertiesJson = compact(render(getPropertiesJValue(testFlintIndex)))
        propertiesJson should matchJson(s"""
            | {
            |   "metadataCacheVersion": "${FlintMetadataCache.metadataCacheVersion}",
            |   "refreshInterval": 600,
            |   "sourceTables": ["$testTable"]
            | }
            |""".stripMargin)

        flint.refreshIndex(testFlintIndex)
        compact(render(getPropertiesJValue(testFlintIndex))) should not include "lastRefreshTime"
      }
    }
  }

  private def getPropertiesJValue(indexName: String): JValue = {
    // Convert to scala map because json4s converts java.util.Map into an empty JObject
    // https://github.com/json4s/json4s/issues/392
    val properties = flintIndexMetadataService.getIndexMetadata(indexName).properties.asScala
    Extraction.decompose(properties)(DefaultFormats)
  }
}
