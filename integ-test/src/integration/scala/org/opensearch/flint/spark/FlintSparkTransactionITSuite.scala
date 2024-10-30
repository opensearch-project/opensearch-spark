/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.Base64

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.OpenSearchTransactionSuite
import org.opensearch.flint.common.metadata.log.FlintMetadataLogEntry.IndexState.FAILED
import org.opensearch.flint.core.storage.FlintMetadataLogEntryOpenSearchConverter.constructLogEntry
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.index.seqno.SequenceNumbers.{UNASSIGNED_PRIMARY_TERM, UNASSIGNED_SEQ_NO}
import org.scalatest.matchers.should.Matchers

class FlintSparkTransactionITSuite extends OpenSearchTransactionSuite with Matchers {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_tx_test"
  private val testFlintIndex = getSkippingIndexName(testTable)
  private val testLatestId: String = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createPartitionedAddressTable(testTable)
  }

  override def afterEach(): Unit = {

    /**
     * Todo, if state is not valid, will throw IllegalStateException. Should check flint
     * .isRefresh before cleanup resource. Current solution, (1) try to delete flint index, (2) if
     * failed, delete index itself.
     */
    deleteTestIndex(testFlintIndex)
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
      and contain("lastRefreshStartTime" -> 0)
      and contain("lastRefreshCompleteTime" -> 0)
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

  test("full refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    flint.refreshIndex(testFlintIndex)

    var latest = latestLogEntry(testLatestId)
    val prevJobStartTime = latest("jobStartTime").asInstanceOf[Number].longValue()
    val prevLastRefreshStartTime = latest("lastRefreshStartTime").asInstanceOf[Number].longValue()
    val prevLastRefreshCompleteTime =
      latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue()
    latest should contain("state" -> "active")
    prevJobStartTime should be > 0L
    prevLastRefreshStartTime should be > 0L
    prevLastRefreshCompleteTime should be > prevLastRefreshStartTime

    flint.refreshIndex(testFlintIndex)
    latest = latestLogEntry(testLatestId)
    val jobStartTime = latest("jobStartTime").asInstanceOf[Number].longValue()
    val lastRefreshStartTime = latest("lastRefreshStartTime").asInstanceOf[Number].longValue()
    val lastRefreshCompleteTime =
      latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue()
    jobStartTime should be > prevLastRefreshCompleteTime
    lastRefreshStartTime should be > prevLastRefreshCompleteTime
    lastRefreshCompleteTime should be > lastRefreshStartTime
  }

  test("incremental refresh index") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(
          FlintSparkIndexOptions(
            Map(
              "incremental_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testFlintIndex)
        .create()
      flint.refreshIndex(testFlintIndex)

      var latest = latestLogEntry(testLatestId)
      val prevJobStartTime = latest("jobStartTime").asInstanceOf[Number].longValue()
      val prevLastRefreshStartTime =
        latest("lastRefreshStartTime").asInstanceOf[Number].longValue()
      val prevLastRefreshCompleteTime =
        latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue()
      latest should contain("state" -> "active")
      prevJobStartTime should be > 0L
      prevLastRefreshStartTime should be > 0L
      prevLastRefreshCompleteTime should be > prevLastRefreshStartTime

      flint.refreshIndex(testFlintIndex)
      latest = latestLogEntry(testLatestId)
      val jobStartTime = latest("jobStartTime").asInstanceOf[Number].longValue()
      val lastRefreshStartTime = latest("lastRefreshStartTime").asInstanceOf[Number].longValue()
      val lastRefreshCompleteTime =
        latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue()
      jobStartTime should be > prevLastRefreshCompleteTime
      lastRefreshStartTime should be > prevLastRefreshCompleteTime
      lastRefreshCompleteTime should be > lastRefreshStartTime
    }
  }

  test("auto refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")), testFlintIndex)
      .create()
    flint.refreshIndex(testFlintIndex)

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

  test("update full refresh index to auto refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val index = flint.describeIndex(testFlintIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(index, FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
    flint.updateIndex(updatedIndex)
    val latest = latestLogEntry(testLatestId)
    latest should contain("state" -> "refreshing")
    latest("jobStartTime").asInstanceOf[Number].longValue() should be > 0L
    latest("lastRefreshStartTime").asInstanceOf[Number].longValue() shouldBe 0L
    latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue() shouldBe 0L
  }

  test("update auto refresh index to full refresh index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")), testFlintIndex)
      .create()
    flint.refreshIndex(testFlintIndex)

    var latest = latestLogEntry(testLatestId)
    val prevLastRefreshStartTime = latest("lastRefreshStartTime").asInstanceOf[Number].longValue()
    val prevLastRefreshCompleteTime =
      latest("lastRefreshCompleteTime").asInstanceOf[Number].longValue()

    val index = flint.describeIndex(testFlintIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(index, FlintSparkIndexOptions(Map("auto_refresh" -> "false")))
    flint.updateIndex(updatedIndex)
    latest = latestLogEntry(testLatestId)
    latest should contain("state" -> "active")
    latest("lastRefreshStartTime")
      .asInstanceOf[Number]
      .longValue() shouldBe prevLastRefreshStartTime
    latest("lastRefreshCompleteTime")
      .asInstanceOf[Number]
      .longValue() shouldBe prevLastRefreshCompleteTime
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

    // Both index data and metadata log should be vacuumed
    flint.vacuumIndex(testFlintIndex)
    openSearchClient
      .indices()
      .exists(new GetIndexRequest(testFlintIndex), RequestOptions.DEFAULT) shouldBe false
    openSearchClient.exists(
      new GetRequest(testMetaLogIndex, testLatestId),
      RequestOptions.DEFAULT) shouldBe false
  }

  test("should fail to vacuum index if index is not logically deleted") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    the[IllegalStateException] thrownBy {
      flint.vacuumIndex(testFlintIndex)
    } should have message
      s"Index state [active] doesn't satisfy precondition"
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
    } should have message
      s"Flint index $testFlintIndex already exists"
  }

  Seq(
    ("refresh", () => flint.refreshIndex(testFlintIndex)),
    ("delete", () => flint.deleteIndex(testFlintIndex)),
    ("vacuum", () => flint.vacuumIndex(testFlintIndex)),
    ("recover", () => flint.recoverIndex(testFlintIndex))).foreach { case (opName, opAction) =>
    test(s"should clean up metadata log entry when $opName index corrupted") {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .create()

      // Simulate user delete index data directly (move index state to deleted to assert index recreated)
      flint.deleteIndex(testFlintIndex)
      deleteIndex(testFlintIndex)

      // Expect that next API call will clean it up
      latestLogEntry(testLatestId) should contain("state" -> "deleted")
      opAction()
      latestLogEntry(testLatestId) shouldBe empty
    }
  }

  test("should clean up metadata log entry and recreate when creating index corrupted") {
    def createIndex(): Unit = {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .create()
    }
    createIndex()

    // Simulate user delete index data directly (move index state to deleted to assert index recreated)
    flint.deleteIndex(testFlintIndex)
    deleteIndex(testFlintIndex)

    // Expect that create action will clean it up and then recreate
    latestLogEntry(testLatestId) should contain("state" -> "deleted")
    createIndex()
    latestLogEntry(testLatestId) should contain("state" -> "active")
  }

  Seq(
    ("refresh", () => flint.refreshIndex(testFlintIndex)),
    ("delete", () => flint.deleteIndex(testFlintIndex)),
    ("vacuum", () => flint.vacuumIndex(testFlintIndex)),
    ("recover", () => flint.recoverIndex(testFlintIndex))).foreach { case (opName, opAction) =>
    test(s"should clean up metadata log entry when $opName index corrupted and auto refreshing") {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")), testFlintIndex)
        .create()
      flint.refreshIndex(testFlintIndex)

      // Simulate user delete index data directly and index monitor moves index state to failed
      spark.streams.active.find(_.name == testFlintIndex).get.stop()
      deleteIndex(testFlintIndex)
      updateLatestLogEntry(
        constructLogEntry(
          testLatestId,
          UNASSIGNED_SEQ_NO,
          UNASSIGNED_PRIMARY_TERM,
          latestLogEntry(testLatestId).asJava),
        FAILED)

      // Expect that next API call will clean it up
      latestLogEntry(testLatestId) should contain("state" -> "failed")
      opAction()
      latestLogEntry(testLatestId) shouldBe empty
    }
  }

  test(
    "should clean up metadata log entry and recreate when creating index corrupted and auto refreshing") {
    def createIndex(): Unit = {
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")), testFlintIndex)
        .create()
    }

    createIndex()
    flint.refreshIndex(testFlintIndex)

    // Simulate user delete index data directly and index monitor moves index state to failed
    spark.streams.active.find(_.name == testFlintIndex).get.stop()
    deleteIndex(testFlintIndex)
    updateLatestLogEntry(
      constructLogEntry(
        testLatestId,
        UNASSIGNED_SEQ_NO,
        UNASSIGNED_PRIMARY_TERM,
        latestLogEntry(testLatestId).asJava),
      FAILED)

    // Expect that create action clean it up and then recreate
    latestLogEntry(testLatestId) should contain("state" -> "failed")
    createIndex()
    latestLogEntry(testLatestId) should contain("state" -> "active")
  }
}
