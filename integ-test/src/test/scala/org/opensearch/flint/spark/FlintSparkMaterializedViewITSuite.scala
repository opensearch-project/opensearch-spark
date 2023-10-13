/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.opensearch.flint.core.FlintVersion.current
import org.opensearch.flint.spark.FlintSpark.RefreshMode.{FULL, INCREMENTAL}
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkMaterializedViewITSuite extends FlintSparkSuite {

  /** Test table, MV, index name and query */
  private val testTable = "spark_catalog.default.mv_test"
  private val testMvName = "spark_catalog.default.mv_test_metrics"
  private val testFlintIndex = getFlintIndexName(testMvName)
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
    flint.deleteIndex(testFlintIndex)
  }

  test("create materialized view with metadata successfully") {
    val indexOptions =
      FlintSparkIndexOptions(Map("auto_refresh" -> "true", "checkpoint_location" -> "s3://test/"))
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
         |      "columnType": "long"
         |    }],
         |    "options": {
         |      "auto_refresh": "true",
         |      "checkpoint_location": "s3://test/"
         |    },
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

  ignore("full refresh materialized view") {
    flint
      .materializedView()
      .name(testMvName)
      .query(testQuery)
      .create()

    flint.refreshIndex(testFlintIndex, FULL)

    val indexData = flint.queryIndex(testFlintIndex)
    checkAnswer(
      indexData,
      Seq(
        Row(timestamp("2023-10-01 00:00:00"), 1),
        Row(timestamp("2023-10-01 00:10:00"), 2),
        Row(timestamp("2023-10-01 01:00:00"), 1),
        Row(timestamp("2023-10-01 02:00:00"), 1)))
  }

  test("incremental refresh materialized view") {
    withTempDir { checkpointDir =>
      val indexOptions = FlintSparkIndexOptions(
        Map("auto_refresh" -> "true", "checkpoint_location" -> checkpointDir.getAbsolutePath))
      flint
        .materializedView()
        .name(testMvName)
        .query(testQuery)
        .options(indexOptions)
        .create()

      flint
        .refreshIndex(testFlintIndex, INCREMENTAL)
        .map(awaitStreamingComplete)
        .orElse(throw new RuntimeException)

      val indexData = flint.queryIndex(testFlintIndex).select("startTime", "count")
      checkAnswer(
        indexData,
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

  private def timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}
