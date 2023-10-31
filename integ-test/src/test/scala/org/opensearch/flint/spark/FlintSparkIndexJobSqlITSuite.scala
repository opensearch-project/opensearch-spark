/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.io.File

import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.should.Matchers

class FlintSparkIndexJobSqlITSuite extends FlintSparkSuite with Matchers {

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

  override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testSkippingIndex)
    flint.deleteIndex(testCoveringIndex)
    flint.deleteIndex(testMvIndex)
  }

  test("recover skipping index refresh job") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (name VALUE_SET)
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           |""".stripMargin)

      // Wait complete
      var job = spark.streams.active.head
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      // Check result size
      flint.queryIndex(testSkippingIndex).collect() should have size 5

      // Stop job
      job.stop()

      // Append 1 row
      sql(
        s"INSERT INTO $testTable VALUES (TIMESTAMP '2023-10-01 05:00:00', 'F', 35, 'Vancouver')")

      // Recover job
      sql(s"RECOVER INDEX JOB $testSkippingIndex")

      // Wait complete
      job = spark.streams.active.head
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      // Check size again
      flint.queryIndex(testSkippingIndex).collect() should have size 6
    }
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
