/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.FAILED
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

class FlintSparkIndexJobITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testIndex)
  }

  test("recover should exit if index doesn't exist") {
    flint.recoverIndex("non_exist_index") shouldBe false
  }

  test("recover should exit if index is not auto refreshed") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()
    flint.recoverIndex(testIndex) shouldBe false
  }

  test("recover should succeed if index exists and is auto refreshed") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    flint.recoverIndex(testIndex) shouldBe true
    spark.streams.active.exists(_.name == testIndex)
  }

  test("recover should succeed even if index is in failed state") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    val latestId = Base64.getEncoder.encodeToString(testIndex.getBytes)
    updateLatestLogEntry(
      new FlintMetadataLogEntry(latestId, 1, 1, latestLogEntry(latestId).asJava),
      FAILED)

    flint.recoverIndex(testIndex) shouldBe true
    spark.streams.active.exists(_.name == testIndex)
  }
}
