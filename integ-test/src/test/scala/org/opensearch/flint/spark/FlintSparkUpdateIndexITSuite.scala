/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.native.JsonMethods._
import org.opensearch.client.RequestOptions
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.flint.spark.sql.FlintSparkSqlAstBuilder.updateIndex
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.DeleteByQueryRequest
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FlintSparkUpdateIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeEach(): Unit = {
    super.beforeEach()
    createPartitionedMultiRowTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testIndex)
    sql(s"DROP TABLE $testTable")
  }

  test("update index with index options successfully") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addValueSet("address")
        .options(FlintSparkIndexOptions(Map(
          "auto_refresh" -> "false",
          "incremental_refresh" -> "true",
          "refresh_interval" -> "1 Minute",
          "checkpoint_location" -> checkpointDir.getAbsolutePath,
          "index_settings" -> "{\"number_of_shards\": 3,\"number_of_replicas\": 2}")))
        .create()
      flint.describeIndex(testIndex) shouldBe defined

      val updateOptions = Map(
        "auto_refresh" -> "true",
        "incremental_refresh" -> "false",
        "refresh_interval" -> "5 Minute"
      )
      updateIndex(flint, testIndex, updateOptions)

      val readNewIndex = flint.describeIndex(testIndex)
      readNewIndex shouldBe defined

      val optionJson = compact(render(parse(readNewIndex.get.metadata().getContent) \ "_meta" \ "options"))
      optionJson should matchJson(
        s"""
          | {
          |   "auto_refresh": "true",
          |   "incremental_refresh": "false",
          |   "refresh_interval": "5 Minute",
          |   "checkpoint_location": "${checkpointDir.getAbsolutePath}",
          |   "index_settings": "{\\\"number_of_shards\\\": 3,\\\"number_of_replicas\\\": 2}"
          | }
          |""".stripMargin)

      // Load index options from index mapping (verify OS index setting in SQL IT)
      readNewIndex.get.options.autoRefresh() shouldBe true
      readNewIndex.get.options.incrementalRefresh() shouldBe false
      readNewIndex.get.options.refreshInterval() shouldBe Some("5 Minute")
      readNewIndex.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
      readNewIndex.get.options.indexSettings() shouldBe
        Some("{\"number_of_shards\": 3,\"number_of_replicas\": 2}")
    }
  }

  test("should fail if update index without updating auto_refresh option") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("incremental_refresh" -> "true"))
    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("auto_refresh" -> "true"))
    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("checkpoint_location" -> "s3a://test/"))
    the[IllegalArgumentException] thrownBy
      updateIndex(
        flint,
        testIndex,
        Map(
          "auto_refresh" -> "true",
          "checkpoint_location" -> "s3a://test/"))

    deleteTestIndex(testIndex)

    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("incremental_refresh" -> "true"))
    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("auto_refresh" -> "false"))
    the[IllegalArgumentException] thrownBy
      updateIndex(flint, testIndex, Map("checkpoint_location" -> "s3a://test/"))
    the[IllegalArgumentException] thrownBy
      updateIndex(
        flint,
        testIndex,
        Map(
          "auto_refresh" -> "false",
          "checkpoint_location" -> "s3a://test/"))
  }

  test("update full refresh index to auto refresh should start job") {
    // Create full refresh Flint index
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    spark.streams.active.find(_.name == testIndex) shouldBe empty
    flint.queryIndex(testIndex).collect().toSet should have size 0

    // Update Flint index to auto refresh and wait for complete
    val jobId = updateIndex(flint, testIndex, Map("auto_refresh" -> "true"))
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    flint.queryIndex(testIndex).collect().toSet should have size 2
  }

  test("update incremental refresh index to auto refresh should start job") {
    withTempDir { checkpointDir =>
      // Create incremental refresh Flint index and wait for complete
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(
          FlintSparkIndexOptions(
            Map(
              "incremental_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()

      flint.refreshIndex(testIndex) shouldBe empty
      spark.streams.active.find(_.name == testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally and generate a new source file
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)
      sql(
        s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 35, 'Vancouver')
           | """.stripMargin)

      // Update Flint index to auto refresh and wait for complete
      val jobId = updateIndex(
        flint,
        testIndex,
        Map(
          "auto_refresh" -> "true",
          "incremental_refresh" -> "false"))
      jobId shouldBe defined

      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      // Expect to only refresh the new file
      flint.queryIndex(testIndex).collect().toSet should have size 1
    }
  }

  test("update auto refresh index to full refresh should stop job") {
    // Create auto refresh Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    val jobId = flint.refreshIndex(testIndex)
    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    flint.queryIndex(testIndex).collect().toSet should have size 2

    // Update Flint index to full refresh
    updateIndex(flint, testIndex, Map("auto_refresh" -> "false")) shouldBe empty

    // Expect refresh job to be stopped
    spark.streams.active.find(_.name == testIndex) shouldBe empty

    // Generate a new source file
    sql(
      s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=4)
         | VALUES ('Hello', 35, 'Vancouver')
         | """.stripMargin)

    // Index shouldn't be refreshed
    flint.queryIndex(testIndex).collect().toSet should have size 2

    // Full refresh after update
    flint.refreshIndex(testIndex) shouldBe empty
    flint.queryIndex(testIndex).collect().toSet should have size 3
  }

  test("update auto refresh index to incremental refresh should stop job") {
    withTempDir { checkpointDir =>
      // Create auto refresh Flint index and wait for complete
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()

      val jobId = flint.refreshIndex(testIndex)
      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Update Flint index to incremental refresh
      updateIndex(
        flint,
        testIndex,
        Map(
          "auto_refresh" -> "false",
          "incremental_refresh" -> "true")) shouldBe empty

      // Expect refresh job to be stopped
      spark.streams.active.find(_.name == testIndex) shouldBe empty

      // Generate a new source file
      sql(
        s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 35, 'Vancouver')
           | """.stripMargin)

      // Index shouldn't be refreshed
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)

      // Expect to only refresh the new file
      flint.refreshIndex(testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 1
    }
  }
}
