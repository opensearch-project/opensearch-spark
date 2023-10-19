/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.File

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex

import org.apache.spark.sql.Row

class FlintSparkIndexJobSqlITSuite extends FlintSparkSuite {

  private val testTable = "spark_catalog.default.index_job_test"
  private val testSkippingIndex = FlintSparkSkippingIndex.getSkippingIndexName(testTable)

  /** Covering index names */
  private val testIndex = "test"
  private val testCoveringIndex = FlintSparkCoveringIndex.getFlintIndexName(testIndex, testTable)

  /** Materialized view names and query */
  private val testMv = "spark_catalog.default.mv_test"
  private val testMvQuery = s"SELECT name, age FROM $testTable"
  private val testMvIndex = FlintSparkMaterializedView.getFlintIndexName(testMv)

  override def beforeAll(): Unit = {
    super.beforeAll()

    createTimeSeriesTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()

    flint.deleteIndex(testSkippingIndex)
    flint.deleteIndex(testCoveringIndex)
    flint.deleteIndex(testMvIndex)
  }

  test("show all index jobs") {
    startSkippingIndexJob()
    checkAnswer(
      sql("SHOW INDEX JOBS"),
      Seq(Row(testSkippingIndex, testSkippingIndex, testTable, "{}")))

    startCoveringIndexJob()
    checkAnswer(
      sql("SHOW INDEX JOBS"),
      Seq(
        Row(testSkippingIndex, testSkippingIndex, testTable, "{}"),
        Row(testCoveringIndex, testIndex, testTable, "{}")))

    withTempDir { checkpointDir =>
      startMaterializedViewIndexJob(checkpointDir)
      checkAnswer(
        sql("SHOW INDEX JOBS"),
        Seq(
          Row(testSkippingIndex, testSkippingIndex, testTable, "{}"),
          Row(testCoveringIndex, testIndex, testTable, "{}"),
          Row(testMvIndex, testMv, testMvQuery, "{}")))
    }
  }

  // empty result cause exception. fixed in other PR.
  ignore("should return emtpy if no index job") {
    checkAnswer(sql("SHOW INDEX JOBS"), Seq.empty)
  }

  private def startSkippingIndexJob(): Unit = {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (name VALUE_SET)
           | WITH (auto_refresh = true)
           |""".stripMargin)
  }

  private def startCoveringIndexJob(): Unit = {
    sql(s"""
         | CREATE INDEX $testIndex ON $testTable
         | (time, name)
         | WITH (auto_refresh = true)
         |""".stripMargin)
  }

  private def startMaterializedViewIndexJob(checkpointDir: File): Unit = {
    sql(s"""
         | CREATE MATERIALIZED VIEW $testMv
         | AS
         | $testMvQuery
         | WITH (
         |   auto_refresh = true,
         |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
         | )
         |""".stripMargin)
  }
}
