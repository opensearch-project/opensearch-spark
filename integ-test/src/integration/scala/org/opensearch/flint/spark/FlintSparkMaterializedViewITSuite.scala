/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp
import java.util.Base64

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.action.get.GetRequest
import org.opensearch.client.RequestOptions
import org.opensearch.flint.common.FlintVersion.current
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchIndexMetadataService, OpenSearchClientUtils}
import org.opensearch.flint.spark.FlintSparkIndex.quotedTableName
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.{extractSourceTablesFromQuery, getFlintIndexName, getSourceTablesFromMetadata, MV_INDEX_TYPE}
import org.opensearch.flint.spark.scheduler.OpenSearchAsyncQueryScheduler
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkMaterializedViewITSuite extends FlintSparkSuite {

  /** Test table, MV, index name and query */
  private val testTable = "spark_catalog.default.mv_test"
  private val testMvName = "spark_catalog.default.mv_test_metrics"
  private val testFlintIndex = getFlintIndexName(testMvName)
  private val testLatestId = Base64.getEncoder.encodeToString(testFlintIndex.getBytes)
  private val testQuery =
    s"""
       | SELECT
       |   window.start AS startTime,
       |   COUNT(*) AS count
       | FROM $testTable
       | GROUP BY TUMBLE(time, '10 Minutes')
       |""".stripMargin

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTimeSeriesTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(testFlintIndex)
  }

  test("extract source table names from materialized view source query successfully") {
    val testComplexQuery = s"""
        | SELECT *
        | FROM (
        |   SELECT 1
        |   FROM table1
        |   LEFT JOIN `table2`
        | )
        | UNION ALL
        | SELECT 1
        | FROM spark_catalog.default.`table/3`
        | INNER JOIN spark_catalog.default.`table.4`
        |""".stripMargin
    extractSourceTablesFromQuery(flint.spark, testComplexQuery) should contain theSameElementsAs
      Array(
        "spark_catalog.default.table1",
        "spark_catalog.default.table2",
        "spark_catalog.default.`table/3`",
        "spark_catalog.default.`table.4`")

    extractSourceTablesFromQuery(flint.spark, "SELECT 1") should have size 0
  }

  test("get source table names from index metadata successfully") {
    val mv = FlintSparkMaterializedView(
      "spark_catalog.default.mv",
      s"SELECT 1 FROM $testTable",
      Array(testTable),
      Map("1" -> "integer"))
    val metadata = mv.metadata()
    getSourceTablesFromMetadata(metadata) should contain theSameElementsAs Array(testTable)
  }

  test("get source table names from deserialized metadata successfully") {
    val metadata = FlintOpenSearchIndexMetadataService.deserialize(s""" {
        |   "_meta": {
        |     "kind": "$MV_INDEX_TYPE",
        |     "properties": {
        |       "sourceTables": [
        |         "$testTable"
        |       ]
        |     }
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin)
    getSourceTablesFromMetadata(metadata) should contain theSameElementsAs Array(testTable)
  }

  test("get empty source tables from invalid field in metadata") {
    val metadataWrongType = FlintOpenSearchIndexMetadataService.deserialize(s""" {
        |   "_meta": {
        |     "kind": "$MV_INDEX_TYPE",
        |     "properties": {
        |       "sourceTables": "$testTable"
        |     }
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin)
    val metadataMissingField = FlintOpenSearchIndexMetadataService.deserialize(s""" {
        |   "_meta": {
        |     "kind": "$MV_INDEX_TYPE",
        |     "properties": { }
        |   },
        |   "properties": {
        |     "age": {
        |       "type": "integer"
        |     }
        |   }
        | }
        |""".stripMargin)

    getSourceTablesFromMetadata(metadataWrongType) shouldBe empty
    getSourceTablesFromMetadata(metadataMissingField) shouldBe empty
  }

  test("create materialized view with metadata successfully") {
    withTempDir { checkpointDir =>
      val indexOptions =
        FlintSparkIndexOptions(
          Map(
            "auto_refresh" -> "true",
            "checkpoint_location" -> checkpointDir.getAbsolutePath,
            "watermark_delay" -> "30 Seconds"))
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(indexOptions, testFlintIndex)
        .create()

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined
      FlintOpenSearchIndexMetadataService.serialize(index.get.metadata()) should matchJson(s"""
           | {
           |  "_meta": {
           |    "version": "${current()}",
           |    "name": "spark_catalog.default.mv_test_metrics",
           |    "kind": "mv",
           |    "source": "$testQuery",
           |    "indexedColumns": [
           |    {
           |      "columnName": "startTime",
           |      "columnType": "timestamp"
           |    },{
           |      "columnName": "count",
           |      "columnType": "bigint"
           |    }],
           |    "options": {
           |      "auto_refresh": "true",
           |      "incremental_refresh": "false",
           |      "checkpoint_location": "${checkpointDir.getAbsolutePath}",
           |      "watermark_delay": "30 Seconds",
           |      "scheduler_mode":"internal"
           |    },
           |    "latestId": "$testLatestId",
           |    "properties": {
           |      "sourceTables": ["$testTable"]
           |    }
           |  },
           |  "properties": {
           |    "startTime": {
           |      "type": "date",
           |      "format": "strict_date_optional_time_nanos"
           |    },
           |    "count": {
           |      "type": "long"
           |    }
           |  }
           | }
           |""".stripMargin)
    }
  }

  test("create materialized view should parse source tables successfully") {
    val indexOptions = FlintSparkIndexOptions(Map.empty)
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .options(indexOptions, testFlintIndex)
      .create()

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined
    index.get
      .asInstanceOf[FlintSparkMaterializedView]
      .sourceTables should contain theSameElementsAs Array(testTable)
  }

  test("create materialized view with default checkpoint location successfully") {
    withTempDir { checkpointDir =>
      setFlintSparkConf(
        FlintSparkConf.CHECKPOINT_LOCATION_ROOT_DIR,
        checkpointDir.getAbsolutePath)

      val indexOptions =
        FlintSparkIndexOptions(Map("auto_refresh" -> "true", "watermark_delay" -> "30 Seconds"))
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(indexOptions, testFlintIndex)
        .create()

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined

      val checkpointLocation = index.get.options.checkpointLocation()
      assert(checkpointLocation.isDefined, "Checkpoint location should be defined")
      assert(
        checkpointLocation.get.contains(testFlintIndex),
        s"Checkpoint location dir should contain ${testFlintIndex}")

      conf.unsetConf(FlintSparkConf.CHECKPOINT_LOCATION_ROOT_DIR.key)
    }
  }

  test("full refresh materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    flint.refreshIndex(testFlintIndex)

    val indexData = flint.queryIndex(testFlintIndex)
    checkAnswer(
      indexData.select("startTime", "count"),
      Seq(
        Row(timestamp("2023-10-01 00:00:00"), 1),
        Row(timestamp("2023-10-01 00:10:00"), 2),
        Row(timestamp("2023-10-01 01:00:00"), 1),
        Row(timestamp("2023-10-01 03:00:00"), 1)))
  }

  test("incremental refresh materialized view") {
    withIncrementalMaterializedView(testQuery) { indexData =>
      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1),
          Row(timestamp("2023-10-01 00:10:00"), 2),
          Row(timestamp("2023-10-01 01:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 02:00:00"), 1)
           */
        ))
    }
  }

  test("incremental refresh materialized view with larger window") {
    val largeWindowQuery =
      s"""
         | SELECT
         |   window.start AS startTime,
         |   COUNT(*) AS count
         | FROM $testTable
         | GROUP BY TUMBLE(time, '1 Hour')
         |""".stripMargin

    withIncrementalMaterializedView(largeWindowQuery) { indexData =>
      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 3),
          Row(timestamp("2023-10-01 01:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 02:00:00"), 1)
           */
        ))
    }
  }

  test("incremental refresh materialized view with filtering aggregate query") {
    val filterQuery =
      s"""
         | SELECT
         |   window.start AS startTime,
         |   COUNT(*) AS count
         | FROM $testTable
         | WHERE address = 'Seattle'
         | GROUP BY TUMBLE(time, '5 Minutes')
         |""".stripMargin

    withIncrementalMaterializedView(filterQuery) { indexData =>
      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 00:10:00"), 1)
           */
        ))
    }
  }

  test("incremental refresh materialized view with non-aggregate query") {
    val nonAggQuery =
      s"""
         | SELECT name, age
         | FROM $testTable
         | WHERE age <= 30
         |""".stripMargin

    withIncrementalMaterializedView(nonAggQuery) { indexData =>
      checkAnswer(indexData.select("name", "age"), Seq(Row("A", 30), Row("B", 20), Row("E", 15)))
    }
  }

  test("update materialized view successfully") {
    withTempDir { checkpointDir =>
      // Create full refresh Flint index
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .create()
      val indexData = flint.queryIndex(testFlintIndex)
      checkAnswer(indexData, Seq())

      // Update Flint index to auto refresh and wait for complete
      val updatedIndex = flint
        .materializedView()
        .copyWithUpdate(
          flint.describeIndex(testFlintIndex).get,
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath,
              "watermark_delay" -> "1 Minute")))
      val jobId = flint.updateIndex(updatedIndex)
      jobId shouldBe defined

      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1),
          Row(timestamp("2023-10-01 00:10:00"), 2),
          Row(timestamp("2023-10-01 01:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 02:00:00"), 1)
           */
        ))
    }
  }

  test("update materialized view successfully with custom checkpoint location") {
    withTempDir { checkpointDir =>
      // 1. Create full refresh MV
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(FlintSparkIndexOptions.empty, testFlintIndex)
        .create()
      var indexData = flint.queryIndex(testFlintIndex)
      checkAnswer(indexData, Seq())

      var index = flint.describeIndex(testFlintIndex)
      var checkpointLocation = index.get.options.checkpointLocation()
      assert(checkpointLocation.isEmpty, "Checkpoint location should not be defined")

      // 2. Update the spark conf with a custom checkpoint location
      setFlintSparkConf(
        FlintSparkConf.CHECKPOINT_LOCATION_ROOT_DIR,
        checkpointDir.getAbsolutePath)

      index = flint.describeIndex(testFlintIndex)
      checkpointLocation = index.get.options.checkpointLocation()
      assert(checkpointLocation.isEmpty, "Checkpoint location should not be defined")

      // 3. Update Flint index to auto refresh and wait for complete
      val updatedIndex = flint
        .materializedView()
        .copyWithUpdate(
          index.get,
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "watermark_delay" -> "1 Minute")))
      val jobId = flint.updateIndex(updatedIndex)
      jobId shouldBe defined

      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      indexData = flint.queryIndex(testFlintIndex)
      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1),
          Row(timestamp("2023-10-01 00:10:00"), 2),
          Row(timestamp("2023-10-01 01:00:00"), 1)
          /*
           * The last row is pending to fire upon watermark
           *   Row(timestamp("2023-10-01 02:00:00"), 1)
           */
        ))

      index = flint.describeIndex(testFlintIndex)

      checkpointLocation = index.get.options.checkpointLocation()
      assert(checkpointLocation.isDefined, "Checkpoint location should be defined")
      assert(
        checkpointLocation.get.contains(testFlintIndex),
        s"Checkpoint location dir should contain ${testFlintIndex}")

      conf.unsetConf(FlintSparkConf.CHECKPOINT_LOCATION_ROOT_DIR.key)
    }
  }

  test("create materialized view with external scheduler") {
    withTempDir { checkpointDir =>
      setFlintSparkConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED, "true")
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "scheduler_mode" -> "external",
              "watermark_delay" -> "1 Minute",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testFlintIndex)
        .create()

      // Verify the job is scheduled
      val client = OpenSearchClientUtils.createClient(new FlintOptions(openSearchOptions.asJava))
      val response = client.get(
        new GetRequest(OpenSearchAsyncQueryScheduler.SCHEDULER_INDEX_NAME, testFlintIndex),
        RequestOptions.DEFAULT)

      response.isExists shouldBe true
      val sourceMap = response.getSourceAsMap

      sourceMap.get("jobId") shouldBe testFlintIndex
      sourceMap
        .get("scheduledQuery") shouldBe s"REFRESH MATERIALIZED VIEW ${quotedTableName(testMvName)}"
      sourceMap.get("enabled") shouldBe true
      sourceMap.get("queryLang") shouldBe "sql"

      val schedule = sourceMap.get("schedule").asInstanceOf[java.util.Map[String, Any]]
      val interval = schedule.get("interval").asInstanceOf[java.util.Map[String, Any]]
      interval.get("period") shouldBe 5
      interval.get("unit") shouldBe "MINUTES"

      // Refresh the index and get the job ID
      val jobId = flint.refreshIndex(testFlintIndex)
      jobId shouldBe None

      val indexData = flint.queryIndex(testFlintIndex)
      checkAnswer(
        indexData.select("startTime", "count"),
        Seq(
          Row(timestamp("2023-10-01 00:00:00"), 1),
          Row(timestamp("2023-10-01 00:10:00"), 2),
          Row(timestamp("2023-10-01 01:00:00"), 1)))

      conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
    }
  }

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)

  private def withIncrementalMaterializedView(query: String)(
      codeBlock: DataFrame => Unit): Unit = {
    withTempDir { checkpointDir =>
      val indexOptions = FlintSparkIndexOptions(
        Map(
          "auto_refresh" -> "true",
          "checkpoint_location" -> checkpointDir.getAbsolutePath,
          "watermark_delay" -> "1 Minute"
        )
      ) // This must be small to ensure window closed soon

      flint
        .materializedView()
        .name(testMvName)
        .query(query)
        .options(indexOptions, testFlintIndex)
        .create()

      flint
        .refreshIndex(testFlintIndex)
        .map(awaitStreamingComplete)
        .orElse(throw new RuntimeException)

      val indexData = flint.queryIndex(testFlintIndex)

      // Execute the code block
      codeBlock(indexData)
    }
  }
}
