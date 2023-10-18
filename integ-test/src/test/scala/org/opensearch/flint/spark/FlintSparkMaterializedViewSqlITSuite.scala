/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.sql.Timestamp

import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row

class FlintSparkMaterializedViewSqlITSuite extends FlintSparkSuite {

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

  test("create materialized view with auto refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE MATERIALIZED VIEW $testMvName
           | AS $testQuery
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           |""".stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == testFlintIndex)
      job shouldBe defined
      failAfter(streamingTimeout) {
        job.get.processAllAvailable()
      }

      flint.describeIndex(testFlintIndex) shouldBe defined
      checkAnswer(
        flint.queryIndex(testFlintIndex).select("startTime", "count"),
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
