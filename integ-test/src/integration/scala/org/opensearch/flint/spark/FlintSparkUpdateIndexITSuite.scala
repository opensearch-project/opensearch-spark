/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.native.JsonMethods._
import org.opensearch.client.RequestOptions
import org.opensearch.flint.core.storage.FlintOpenSearchIndexMetadataService
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.DeleteByQueryRequest
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkUpdateIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeEach(): Unit = {
    super.beforeEach()
    createPartitionedMultiRowAddressTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testIndex)
    sql(s"DROP TABLE $testTable")
    conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
  }

  test("update index with index options successfully") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addValueSet("address")
        .options(
          FlintSparkIndexOptions(Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "refresh_interval" -> "1 Minute",
            "checkpoint_location" -> checkpointDir.getAbsolutePath,
            "index_settings" -> "{\"number_of_shards\": 3,\"number_of_replicas\": 2}")),
          testIndex)
        .create()
      val indexInitial = flint.describeIndex(testIndex).get

      // Update index options
      val updateOptions =
        FlintSparkIndexOptions(Map("auto_refresh" -> "true", "incremental_refresh" -> "false"))
      val updatedIndex = flint.skippingIndex().copyWithUpdate(indexInitial, updateOptions)
      flint.updateIndex(updatedIndex)

      // Verify index after update
      val indexFinal = flint.describeIndex(testIndex).get
      val optionJson =
        compact(
          render(
            parse(
              FlintOpenSearchIndexMetadataService.serialize(
                indexFinal.metadata())) \ "_meta" \ "options"))
      optionJson should matchJson(s"""
          | {
          |   "auto_refresh": "true",
          |   "scheduler_mode": "internal",
          |   "incremental_refresh": "false",
          |   "refresh_interval": "1 Minute",
          |   "checkpoint_location": "${checkpointDir.getAbsolutePath}",
          |   "index_settings": "{\\\"number_of_shards\\\": 3,\\\"number_of_replicas\\\": 2}"
          | }
          |""".stripMargin)

      // Load index options from index mapping (verify OS index setting in SQL IT)
      indexFinal.options.autoRefresh() shouldBe true
      indexFinal.options.incrementalRefresh() shouldBe false
      indexFinal.options.refreshInterval() shouldBe Some("1 Minute")
      indexFinal.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
      indexFinal.options.indexSettings() shouldBe
        Some("{\"number_of_shards\": 3,\"number_of_replicas\": 2}")
    }
  }

  // Test update options validation failure
  Seq(
    (
      "update index without changing auto_refresh option",
      Seq(
        (Map("auto_refresh" -> "true"), Map("auto_refresh" -> "true")),
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/")),
        (Map("auto_refresh" -> "true"), Map("checkpoint_location" -> "s3a://test/")),
        (Map("auto_refresh" -> "true"), Map("watermark_delay" -> "1 Minute")),
        (Map.empty[String, String], Map("auto_refresh" -> "false")),
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "false", "checkpoint_location" -> "s3a://test/")),
        (Map.empty[String, String], Map("incremental_refresh" -> "true")),
        (Map.empty[String, String], Map("checkpoint_location" -> "s3a://test/")))),
    (
      "convert to full refresh with disallowed options",
      Seq(
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "false", "checkpoint_location" -> "s3a://test/")),
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "false", "refresh_interval" -> "5 Minute")),
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "false", "watermark_delay" -> "1 Minute")))),
    (
      "convert to incremental refresh with disallowed options",
      Seq(
        (
          Map("auto_refresh" -> "true"),
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "output_mode" -> "complete")))),
    (
      "convert to auto refresh with disallowed options",
      Seq(
        (Map.empty[String, String], Map("auto_refresh" -> "true", "output_mode" -> "complete")))),
    (
      "convert to invalid refresh mode",
      Seq(
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "true", "incremental_refresh" -> "true")),
        (Map("auto_refresh" -> "true"), Map("incremental_refresh" -> "true")),
        (
          Map("incremental_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "true"))))).foreach { case (testName, testCases) =>
    test(s"should fail if $testName") {
      testCases.foreach { case (initialOptionsMap, updateOptionsMap) =>
        withTempDir { checkpointDir =>
          flint
            .skippingIndex()
            .onTable(testTable)
            .addPartitions("year", "month")
            .options(
              FlintSparkIndexOptions(initialOptionsMap
                .get("checkpoint_location")
                .map(_ =>
                  initialOptionsMap.updated("checkpoint_location", checkpointDir.getAbsolutePath))
                .getOrElse(initialOptionsMap)),
              testIndex)
            .create()
          flint.refreshIndex(testIndex)

          val index = flint.describeIndex(testIndex).get
          the[IllegalArgumentException] thrownBy {
            val updatedIndex = flint
              .skippingIndex()
              .copyWithUpdate(
                index,
                FlintSparkIndexOptions(
                  updateOptionsMap
                    .get("checkpoint_location")
                    .map(_ =>
                      updateOptionsMap
                        .updated("checkpoint_location", checkpointDir.getAbsolutePath))
                    .getOrElse(updateOptionsMap)))
            flint.updateIndex(updatedIndex)
          }

          deleteTestIndex(testIndex)
        }
      }
    }
  }

  // Test update options validation failure with external scheduler
  Seq(
    (
      "update index without changing index option",
      Seq(
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "true")),
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("checkpoint_location" -> "s3a://test/")),
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"))),
      "No index option updated"),
    (
      "update index option when auto_refresh is false",
      Seq(
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "false", "checkpoint_location" -> "s3a://test/")),
        (
          Map.empty[String, String],
          Map("incremental_refresh" -> "true", "checkpoint_location" -> "s3a://test/")),
        (Map.empty[String, String], Map("checkpoint_location" -> "s3a://test/"))),
      "No options can be updated when auto_refresh remains false"),
    (
      "update other index option besides scheduler_mode when auto_refresh is true",
      Seq(
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("watermark_delay" -> "1 Minute"))),
      "Altering index when auto_refresh remains true only allows changing: Set(scheduler_mode). Invalid options"),
    (
      "convert to full refresh with disallowed options",
      Seq(
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "false", "scheduler_mode" -> "internal")),
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "false", "refresh_interval" -> "5 Minute")),
        (
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map("auto_refresh" -> "false", "watermark_delay" -> "1 Minute"))),
      "Altering index to full/incremental refresh only allows changing"),
    (
      "convert to auto refresh with disallowed options",
      Seq(
        (
          Map.empty[String, String],
          Map(
            "auto_refresh" -> "true",
            "output_mode" -> "complete",
            "checkpoint_location" -> "s3a://test/"))),
      "Altering index to auto refresh only allows changing: Set(auto_refresh, watermark_delay, scheduler_mode, " +
        "refresh_interval, incremental_refresh, checkpoint_location). Invalid options: Set(output_mode)"),
    (
      "convert to invalid refresh mode",
      Seq(
        (
          Map.empty[String, String],
          Map(
            "auto_refresh" -> "true",
            "incremental_refresh" -> "true",
            "checkpoint_location" -> "s3a://test/"))),
      "Altering index to auto refresh while incremental refresh remains true"))
    .foreach { case (testName, testCases, expectedErrorMessage) =>
      test(s"should fail if $testName and external scheduler enabled") {
        setFlintSparkConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED, "true")
        testCases.foreach { case (initialOptionsMap, updateOptionsMap) =>
          logInfo(s"initialOptionsMap: ${initialOptionsMap}")
          logInfo(s"updateOptionsMap: ${updateOptionsMap}")

          withTempDir { checkpointDir =>
            flint
              .skippingIndex()
              .onTable(testTable)
              .addPartitions("year", "month")
              .options(
                FlintSparkIndexOptions(
                  initialOptionsMap
                    .get("checkpoint_location")
                    .map(_ =>
                      initialOptionsMap
                        .updated("checkpoint_location", checkpointDir.getAbsolutePath))
                    .getOrElse(initialOptionsMap)),
                testIndex)
              .create()
            flint.refreshIndex(testIndex)

            val index = flint.describeIndex(testIndex).get
            val exception = the[IllegalArgumentException] thrownBy {
              val updatedIndex = flint
                .skippingIndex()
                .copyWithUpdate(
                  index,
                  FlintSparkIndexOptions(
                    updateOptionsMap
                      .get("checkpoint_location")
                      .map(_ =>
                        updateOptionsMap
                          .updated("checkpoint_location", checkpointDir.getAbsolutePath))
                      .getOrElse(updateOptionsMap)))
              flint.updateIndex(updatedIndex)
            }

            exception.getMessage should include(expectedErrorMessage)

            deleteTestIndex(testIndex)
          }
        }
      }
    }

  // Test update options validation success
  Seq(
    (
      "convert to full refresh with allowed options",
      Seq(
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "false"),
          Map(
            "auto_refresh" -> false,
            "incremental_refresh" -> false,
            "refresh_interval" -> None,
            "checkpoint_location" -> None,
            "watermark_delay" -> None)),
        (
          Map("auto_refresh" -> "true"),
          Map("auto_refresh" -> "false", "incremental_refresh" -> "false"),
          Map(
            "auto_refresh" -> false,
            "incremental_refresh" -> false,
            "refresh_interval" -> None,
            "checkpoint_location" -> None,
            "watermark_delay" -> None)))),
    (
      "convert to incremental refresh with allowed options",
      Seq(
        (
          Map("auto_refresh" -> "true"),
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "refresh_interval" -> "1 Minute",
            "checkpoint_location" -> "s3a://test/"),
          Map(
            "auto_refresh" -> false,
            "incremental_refresh" -> true,
            "refresh_interval" -> Some("1 Minute"),
            "checkpoint_location" -> Some("s3a://test/"),
            "watermark_delay" -> None)),
        (
          Map("auto_refresh" -> "true"),
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "checkpoint_location" -> "s3a://test/"),
          Map(
            "auto_refresh" -> false,
            "incremental_refresh" -> true,
            "refresh_interval" -> None,
            "checkpoint_location" -> Some("s3a://test/"),
            "watermark_delay" -> None)),
        (
          Map("auto_refresh" -> "true"),
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "checkpoint_location" -> "s3a://test/",
            "watermark_delay" -> "1 Minute"),
          Map(
            "auto_refresh" -> false,
            "incremental_refresh" -> true,
            "refresh_interval" -> None,
            "checkpoint_location" -> Some("s3a://test/"),
            "watermark_delay" -> Some("1 Minute"))))),
    (
      "convert to auto refresh with allowed options",
      Seq(
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "true", "refresh_interval" -> "1 Minute"),
          Map(
            "auto_refresh" -> true,
            "incremental_refresh" -> false,
            "refresh_interval" -> Some("1 Minute"),
            "checkpoint_location" -> None,
            "watermark_delay" -> None)),
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/"),
          Map(
            "auto_refresh" -> true,
            "incremental_refresh" -> false,
            "refresh_interval" -> None,
            "checkpoint_location" -> Some("s3a://test/"),
            "watermark_delay" -> None)),
        (
          Map.empty[String, String],
          Map("auto_refresh" -> "true", "watermark_delay" -> "1 Minute"),
          Map(
            "auto_refresh" -> true,
            "incremental_refresh" -> false,
            "refresh_interval" -> None,
            "checkpoint_location" -> None,
            "watermark_delay" -> Some("1 Minute")))))).foreach { case (testName, testCases) =>
    test(s"should succeed if $testName") {
      testCases.foreach { case (initialOptionsMap, updateOptionsMap, expectedOptionsMap) =>
        withTempDir { checkpointDir =>
          flint
            .skippingIndex()
            .onTable(testTable)
            .addPartitions("year", "month")
            .options(
              FlintSparkIndexOptions(initialOptionsMap
                .get("checkpoint_location")
                .map(_ =>
                  initialOptionsMap.updated("checkpoint_location", checkpointDir.getAbsolutePath))
                .getOrElse(initialOptionsMap)),
              testIndex)
            .create()
          flint.refreshIndex(testIndex)

          val indexInitial = flint.describeIndex(testIndex).get
          val updatedIndex = flint
            .skippingIndex()
            .copyWithUpdate(
              indexInitial,
              FlintSparkIndexOptions(
                updateOptionsMap
                  .get("checkpoint_location")
                  .map(_ =>
                    updateOptionsMap
                      .updated("checkpoint_location", checkpointDir.getAbsolutePath))
                  .getOrElse(updateOptionsMap)))
          flint.updateIndex(updatedIndex)

          val optionsFinal = flint.describeIndex(testIndex).get.options
          optionsFinal.autoRefresh() shouldBe expectedOptionsMap.get("auto_refresh").get
          optionsFinal
            .incrementalRefresh() shouldBe expectedOptionsMap.get("incremental_refresh").get
          optionsFinal.refreshInterval() shouldBe expectedOptionsMap.get("refresh_interval").get
          optionsFinal.checkpointLocation() shouldBe (expectedOptionsMap
            .get("checkpoint_location")
            .get match {
            case Some(_) => Some(checkpointDir.getAbsolutePath)
            case None => None
          })
          optionsFinal.watermarkDelay() shouldBe expectedOptionsMap.get("watermark_delay").get

          deleteTestIndex(testIndex)
        }
      }
    }
  }

  test("update index should fail if index is updated by others before transaction starts") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .create()

      // This update will be delayed
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndexObsolete = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))

      // This other update finishes first, converting to auto refresh
      flint.updateIndex(
        flint
          .skippingIndex()
          .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true"))))
      // Adding another update to convert to full refresh, so the obsolete update doesn't fail for option validation or state validation
      val indexUpdated = flint.describeIndex(testIndex).get
      flint.updateIndex(
        flint
          .skippingIndex()
          .copyWithUpdate(indexUpdated, FlintSparkIndexOptions(Map("auto_refresh" -> "false"))))

      // This update trying to update an obsolete index should fail
      the[IllegalStateException] thrownBy
        flint.updateIndex(updatedIndexObsolete)

      // Verify index after update
      val indexFinal = flint.describeIndex(testIndex).get
      indexFinal.options.autoRefresh() shouldBe false
      indexFinal.options.checkpointLocation() shouldBe empty
    }
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
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex =
      flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
    val jobId = flint.updateIndex(updatedIndex)
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
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testIndex)
        .create()

      flint.refreshIndex(testIndex) shouldBe empty
      spark.streams.active.find(_.name == testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally and generate a new source file
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)
      sql(s"""
             | INSERT INTO $testTable
             | PARTITION (year=2023, month=4)
             | VALUES ('Hello', 35, 'Vancouver')
             | """.stripMargin)

      // Update Flint index to auto refresh and wait for complete
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "incremental_refresh" -> "false")))
      val jobId = flint.updateIndex(updatedIndex)
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
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")), testIndex)
      .create()

    val jobId = flint.refreshIndex(testIndex)
    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    flint.queryIndex(testIndex).collect().toSet should have size 2

    // Update Flint index to full refresh
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex =
      flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "false")))
    flint.updateIndex(updatedIndex) shouldBe empty

    // Expect refresh job to be stopped
    spark.streams.active.find(_.name == testIndex) shouldBe empty

    // Generate a new source file
    sql(s"""
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
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testIndex)
        .create()

      val jobId = flint.refreshIndex(testIndex)
      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Update Flint index to incremental refresh
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "false", "incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex) shouldBe empty

      // Expect refresh job to be stopped
      spark.streams.active.find(_.name == testIndex) shouldBe empty

      // Generate a new source file
      sql(s"""
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
