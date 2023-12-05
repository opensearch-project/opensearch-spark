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

    /**
     * Todo, if state is not valid, will throw IllegalStateException. Should check flint
     * .isRefresh before cleanup resource. Current solution, (1) try to delete flint index, (2) if
     * failed, delete index itself.
     */
    try {
      flint.deleteIndex(testFlintIndex)
      flint.vacuumIndex(testFlintIndex)
    } catch {
      case _: IllegalStateException => deleteIndex(testFlintIndex)
    }
    super.afterEach()
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

  test("delete and vacuum index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Logical delete index
    flint.deleteIndex(testFlintIndex)
    latestLogEntry(testLatestId) should contain("state" -> "deleted")

    // Vacuum index data and metadata log
    flint.vacuumIndex(testFlintIndex)
    openSearchClient
      .indices()
      .exists(new GetIndexRequest(testFlintIndex), RequestOptions.DEFAULT) shouldBe false
    openSearchClient.exists(
      new GetRequest(testMetaLogIndex, testLatestId),
      RequestOptions.DEFAULT) shouldBe false
  }

  test("should not recreate index if index data still exists") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Simulate that PPL plugin leaves index data as logical deleted
    flint.deleteIndex(testFlintIndex)
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
}
