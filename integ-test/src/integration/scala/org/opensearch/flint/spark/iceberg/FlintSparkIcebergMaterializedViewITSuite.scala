/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.iceberg

import org.opensearch.flint.spark.FlintSparkMaterializedViewSqlITSuite
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.getFlintIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.sql.Row
import org.apache.spark.sql.flint.config.FlintSparkConf

class FlintSparkIcebergMaterializedViewITSuite
    extends FlintSparkMaterializedViewSqlITSuite
    with FlintSparkIcebergSuite {

  private val testTable = s"$catalogName.default.mv_test_iceberg"
  private val testMvName = s"$catalogName.default.mv_test_iceberg_metrics"
  private val testFlintIndex = getFlintIndexName(testMvName)
  private val testQuery =
    s"""
       | SELECT
       |   mymap
       | FROM $testTable
       |""".stripMargin

  override def beforeEach(): Unit = {
    super.beforeAll()
    createIcebergTimeSeriesTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(testFlintIndex)
    sql(s"DROP TABLE $testTable")
    conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)
    conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
  }

  test("create materialized view with map type") {
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
        flint.queryIndex(testFlintIndex).select("mymap"),
        Seq(
          Row(Row("mapvalue2", null, null, null, null)),
          Row(Row(null, "mapvalue5", null, null, null)),
          Row(Row(null, null, "mapvalue3", null, null)),
          Row(Row(null, null, null, "mapvalue4", null)),
          Row(Row(null, null, null, null, "mapvalue1"))
        ))
    }
  }
}
