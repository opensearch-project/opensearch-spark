/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState.DELETED
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.should.Matchers

class FlintSparkTransactionITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_tx_test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testFlintIndex)
  }

  test("create index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    latestLogEntry(testLatestId) should (contain("latestId" -> testLatestId)
      and contain("state" -> "active")
      and contain("jobStartTime" -> 0)
      and contain("dataSourceName" -> testDataSourceName))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val mapping =
      openSearchClient
        .indices()
        .get(new GetIndexRequest(testFlintIndex), RequestOptions.DEFAULT)
        .getMappings
        .get(testFlintIndex)
        .source()
        .string()
    (parse(mapping) \ "_meta" \ "latestId").extract[String] shouldBe testLatestId
  }

  test("manual refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.refreshIndex(testFlintIndex, FULL)

    val latest = latestLogEntry(testLatestId)
    latest should contain("state" -> "active")
    latest("jobStartTime").asInstanceOf[Number].longValue() should be > 0L
  }

  test("incremental refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testFlintIndex, INCREMENTAL)

    // Job start time should be assigned
    var latest = latestLogEntry(testLatestId)
    latest should contain("state" -> "refreshing")
    val prevStartTime = latest("jobStartTime").asInstanceOf[Number].longValue()
    prevStartTime should be > 0L

    // Restart streaming job
    spark.streams.active.head.stop()
    flint.recoverIndex(testFlintIndex)

    // Make sure job start time is updated
    latest = latestLogEntry(testLatestId)
    latest("jobStartTime").asInstanceOf[Number].longValue() should be > prevStartTime
  }

  test("delete index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.deleteIndex(testFlintIndex)

    latestLogEntry(testLatestId) should contain("state" -> "deleted")
  }

  test("should recreate index if logical deleted") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Simulate that user deletes index data manually
    flint.deleteIndex(testFlintIndex)
    latestLogEntry(testLatestId) should contain("state" -> "deleted")

    // Simulate that user recreate the index
    flint
      .skippingIndex()
      .onTable(testTable)
      .addValueSet("name")
      .create()
  }

  test("should not recreate index if index data still exists") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Simulate that PPL plugin leaves index data as logical deleted
    deleteLogically(testLatestId)
    latestLogEntry(testLatestId) should contain("state" -> "deleted")

    // Simulate that user recreate the index but forgot to cleanup index data
    the[IllegalStateException] thrownBy {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addValueSet("name")
        .create()
    } should have message s"Flint index $testFlintIndex already exists"
  }

  private def deleteLogically(latestId: String): Unit = {
    val response = openSearchClient
      .get(new GetRequest(testMetaLogIndex, latestId), RequestOptions.DEFAULT)

    val latest = new FlintMetadataLogEntry(
      latestId,
      response.getSeqNo,
      response.getPrimaryTerm,
      response.getSourceAsMap)
    updateLatestLogEntry(latest, DELETED)
  }
}
