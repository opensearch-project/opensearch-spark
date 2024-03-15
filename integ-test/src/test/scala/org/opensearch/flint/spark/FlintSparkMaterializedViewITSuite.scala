/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp
import java.util.Base64

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.{DataFrame, Row}

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

  test("create materialized view with metadata successfully") {
    val indexOptions =
      FlintSparkIndexOptions(
        Map(
          "auto_refresh" -> "true",
          "checkpoint_location" -> "s3://test/",
          "watermark_delay" -> "30 Seconds"))
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .options(indexOptions)
      .create()

    val index = flint.describeIndex(testFlintIndex)
    index shouldBe defined
    index.get.metadata().getContent should matchJson(s"""
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
         |      "checkpoint_location": "s3://test/",
         |      "watermark_delay": "30 Seconds"
         |    },
         |    "latestId": "$testLatestId",
         |    "properties": {}
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
        .options(indexOptions)
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
