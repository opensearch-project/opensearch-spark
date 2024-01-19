/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers

class FlintSparkIndexJobITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.index_job_test"
  private val testIndex = getSkippingIndexName(testTable)
  private val latestId = Base64.getEncoder.encodeToString(testIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    deleteTestIndex(testIndex)
    super.afterEach()
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
    latestLogEntry(latestId) should contain("state" -> "refreshing")
  }

  Seq(EMPTY, CREATING, DELETING, DELETED, RECOVERING, UNKNOWN).foreach { state =>
    test(s"recover should fail if index is in $state state") {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year")
        .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
        .create()

      updateLatestLogEntry(
        new FlintMetadataLogEntry(
          latestId,
          UNASSIGNED_SEQ_NO,
          UNASSIGNED_PRIMARY_TERM,
          latestLogEntry(latestId).asJava),
        state)

      assertThrows[IllegalStateException] {
        flint.recoverIndex(testIndex) shouldBe true
      }
    }
  }

  Seq(ACTIVE, REFRESHING, FAILED).foreach { state =>
    test(s"recover should succeed if index is in $state state") {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year")
        .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
        .create()

      updateLatestLogEntry(
        new FlintMetadataLogEntry(
          latestId,
          UNASSIGNED_SEQ_NO,
          UNASSIGNED_PRIMARY_TERM,
          latestLogEntry(latestId).asJava),
        state)

      flint.recoverIndex(testIndex) shouldBe true
      spark.streams.active.exists(_.name == testIndex)
      latestLogEntry(latestId) should contain("state" -> "refreshing")
    }
  }
}
