/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
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

  it should "construct from FlintMetadata" in {
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

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe "1.0"
    metadataCache.refreshInterval.get shouldBe 600
    metadataCache.sourceTables shouldBe Array(FlintMetadataCache.mockTableName)
    metadataCache.lastRefreshTime.get shouldBe 1234567890123L
  }

  it should "construct from FlintMetadata excluding invalid fields" in {
    // Set auto_refresh = false and lastRefreshCompleteTime = 0
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
      .copy(latestLogEntry = Some(flintMetadataLogEntry.copy(lastRefreshCompleteTime = 0L)))

    val metadataCache = FlintMetadataCache(metadata)
    metadataCache.metadataCacheVersion shouldBe "1.0"
    metadataCache.refreshInterval shouldBe empty
    metadataCache.sourceTables shouldBe Array(FlintMetadataCache.mockTableName)
    metadataCache.lastRefreshTime shouldBe empty
  }
}
