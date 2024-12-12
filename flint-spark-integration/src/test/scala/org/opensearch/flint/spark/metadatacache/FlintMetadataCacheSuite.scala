/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.COVERING_INDEX_TYPE
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.MV_INDEX_TYPE
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.SKIPPING_INDEX_TYPE
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FlintMetadataCacheSuite extends AnyFlatSpec with Matchers {
  val flintMetadataLogEntry = FlintMetadataLogEntry(
    "id",
    0L,
    0L,
    1234567890123L,
    FlintMetadataLogEntry.IndexState.ACTIVE,
    Map.empty[String, Any],
    "",
    Map.empty[String, Any])

  it should "construct from skipping index FlintMetadata" in {
    val content =
      s""" {
        |   "_meta": {
        |     "kind": "$SKIPPING_INDEX_TYPE",
        |     "source": "spark_catalog.default.test_table",
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

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe FlintMetadataCache.metadataCacheVersion
    metadataCache.refreshInterval.get shouldBe 600
    metadataCache.sourceTables shouldBe Array("spark_catalog.default.test_table")
    metadataCache.lastRefreshTime.get shouldBe 1234567890123L
  }

  it should "construct from covering index FlintMetadata" in {
    val content =
      s""" {
        |   "_meta": {
        |     "kind": "$COVERING_INDEX_TYPE",
        |     "source": "spark_catalog.default.test_table",
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

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe FlintMetadataCache.metadataCacheVersion
    metadataCache.refreshInterval.get shouldBe 600
    metadataCache.sourceTables shouldBe Array("spark_catalog.default.test_table")
    metadataCache.lastRefreshTime.get shouldBe 1234567890123L
  }

  it should "construct from materialized view FlintMetadata" in {
    val testQuery =
      "SELECT 1 FROM spark_catalog.default.test_table UNION SELECT 1 FROM spark_catalog.default.another_table"
    val content =
      s""" {
        |   "_meta": {
        |     "kind": "$MV_INDEX_TYPE",
        |     "source": "$testQuery",
        |     "options": {
        |       "auto_refresh": "true",
        |       "refresh_interval": "10 Minutes"
        |     },
        |     "properties": {
        |       "sourceTables": [
        |         "spark_catalog.default.test_table",
        |         "spark_catalog.default.another_table"
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

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe FlintMetadataCache.metadataCacheVersion
    metadataCache.refreshInterval.get shouldBe 600
    metadataCache.sourceTables shouldBe Array(
      "spark_catalog.default.test_table",
      "spark_catalog.default.another_table")
    metadataCache.sourceQuery.get shouldBe testQuery
    metadataCache.lastRefreshTime.get shouldBe 1234567890123L
  }

  it should "construct from FlintMetadata excluding invalid fields" in {
    // Set auto_refresh = false and lastRefreshCompleteTime = 0
    val content =
      s""" {
        |   "_meta": {
        |     "kind": "$SKIPPING_INDEX_TYPE",
        |     "source": "spark_catalog.default.test_table",
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
      .copy(latestLogEntry = Some(flintMetadataLogEntry.copy(lastRefreshCompleteTime = 0L)))

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe FlintMetadataCache.metadataCacheVersion
    metadataCache.refreshInterval shouldBe empty
    metadataCache.sourceTables shouldBe Array("spark_catalog.default.test_table")
    metadataCache.sourceQuery shouldBe empty
    metadataCache.lastRefreshTime shouldBe empty
  }
}
