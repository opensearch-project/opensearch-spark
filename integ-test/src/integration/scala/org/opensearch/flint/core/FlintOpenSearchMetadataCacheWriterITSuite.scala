/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

import java.util.List

import scala.collection.JavaConverters._

import org.opensearch.flint.core.metadata.FlintMetadataCache
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

  // TODO: don't use mock; fix tests
  private val mockMetadataCacheData = FlintMetadataCache(
    "1.0",
    Some(900),
    Array(
      "dataSourceName.default.logGroups(logGroupIdentifier:['arn:aws:logs:us-east-1:123456:test-llt-xa', 'arn:aws:logs:us-east-1:123456:sample-lg-1'])"),
    Some(1727395328283L))

  override def beforeAll(): Unit = {
    super.beforeAll()
    setFlintSparkConf(FlintSparkConf.METADATA_CACHE_WRITE, "true")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // TODO: unset if default is false
    // conf.unsetConf(FlintSparkConf.METADATA_CACHE_WRITE.key)
  }

  test("write metadata cache to index mappings") {
    val indexName = "flint_test_index"
    val metadata = FlintOpenSearchIndexMetadataService.deserialize("{}")
    flintClient.createIndex(indexName, metadata)
    flintMetadataCacheWriter.updateMetadataCache(indexName, metadata)

    val properties = flintIndexMetadataService.getIndexMetadata(indexName).properties
    properties should have size 4
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      mockMetadataCacheData.metadataCacheVersion),
    Entry("refreshInterval", mockMetadataCacheData.refreshInterval.get),
    Entry("lastRefreshTime", mockMetadataCacheData.lastRefreshTime.get))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs mockMetadataCacheData.sourceTables
  }

  test("write metadata cache to index mappings and preserve other index metadata") {
    val indexName = "test_update"
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

    val metadata = FlintOpenSearchIndexMetadataService.deserialize(content)
    flintClient.createIndex(indexName, metadata)

    flintIndexMetadataService.updateIndexMetadata(indexName, metadata)
    flintMetadataCacheWriter.updateMetadataCache(indexName, metadata)

    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(indexName).name shouldBe empty
    flintIndexMetadataService.getIndexMetadata(indexName).schema should have size 1
    var properties = flintIndexMetadataService.getIndexMetadata(indexName).properties
    properties should have size 4
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      mockMetadataCacheData.metadataCacheVersion),
    Entry("refreshInterval", mockMetadataCacheData.refreshInterval.get),
    Entry("lastRefreshTime", mockMetadataCacheData.lastRefreshTime.get))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs mockMetadataCacheData.sourceTables

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

    val newMetadata = FlintOpenSearchIndexMetadataService.deserialize(newContent)
    flintIndexMetadataService.updateIndexMetadata(indexName, newMetadata)
    flintMetadataCacheWriter.updateMetadataCache(indexName, newMetadata)

    flintIndexMetadataService.getIndexMetadata(indexName).kind shouldBe "test_kind"
    flintIndexMetadataService.getIndexMetadata(indexName).name shouldBe "test_name"
    flintIndexMetadataService.getIndexMetadata(indexName).schema should have size 1
    properties = flintIndexMetadataService.getIndexMetadata(indexName).properties
    properties should have size 4
    properties should contain allOf (Entry(
      "metadataCacheVersion",
      mockMetadataCacheData.metadataCacheVersion),
    Entry("refreshInterval", mockMetadataCacheData.refreshInterval.get),
    Entry("lastRefreshTime", mockMetadataCacheData.lastRefreshTime.get))
    properties
      .get("sourceTables")
      .asInstanceOf[List[String]]
      .toArray should contain theSameElementsAs mockMetadataCacheData.sourceTables
  }
}
