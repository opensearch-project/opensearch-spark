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

  private val icebergTestTable = s"$catalogName.default.mv_test_iceberg"
  private val icebergTestMvName = s"$catalogName.default.mv_test_iceberg_metrics"
  private val icebergTestFlintIndex = getFlintIndexName(icebergTestMvName)
  private val icebergTestQuery =
    s"""
       | SELECT
       |   mymap
       | FROM $icebergTestTable
       |""".stripMargin

  override def beforeEach(): Unit = {
    super.beforeAll()
    createIcebergTimeSeriesTable(icebergTestTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteTestIndex(icebergTestFlintIndex)
    sql(s"DROP TABLE $icebergTestTable")
    conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)
    conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
  }

  test("create materialized view with map type") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE MATERIALIZED VIEW $icebergTestMvName
             | AS $icebergTestQuery
             | WITH (
             |   auto_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             |""".stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == icebergTestFlintIndex)
      job shouldBe defined
      failAfter(streamingTimeout) {
        job.get.processAllAvailable()
      }

      flint.describeIndex(icebergTestFlintIndex) shouldBe defined
      checkAnswer(
        flint.queryIndex(icebergTestFlintIndex).select("mymap"),
        Seq(
          Row(Row("mapvalue2", null, null, null, null)),
          Row(Row(null, "mapvalue5", null, null, null)),
          Row(Row(null, null, "mapvalue3", null, null)),
          Row(Row(null, null, null, "mapvalue4", null)),
          Row(Row(null, null, null, null, "mapvalue1"))))
    }
  }
}
