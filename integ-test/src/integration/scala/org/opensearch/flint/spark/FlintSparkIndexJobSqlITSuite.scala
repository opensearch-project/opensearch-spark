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

import org.apache.spark.sql.Row

/**
 * This suite doesn't enable transaction to avoid side effect of scheduled update task.
 */
class FlintSparkIndexJobSqlITSuite extends FlintSparkSuite with Matchers {

  private val testTable = "spark_catalog.default.index_job_test"
  private val testSkippingIndex = FlintSparkSkippingIndex.getSkippingIndexName(testTable)

  /** Covering index names */
  private val testIndex = "test_ci"
  private val testCoveringIndex = FlintSparkCoveringIndex.getFlintIndexName(testIndex, testTable)

  /** Materialized view names and query */
  private val testMv = "spark_catalog.default.mv_test"
  private val testMvQuery = s"SELECT name, age FROM $testTable"
  private val testMvIndex = FlintSparkMaterializedView.getFlintIndexName(testMv)

  test("recover skipping index refresh job") {
    withFlintIndex(testSkippingIndex) { assertion =>
      assertion
        .run { checkpointDir =>
          s""" CREATE SKIPPING INDEX ON $testTable
             | (name VALUE_SET)
             | WITH (
             |   auto_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             |""".stripMargin
        }
        .assertIndexData(indexData => indexData should have size 5)
        .stopStreamingJob()
        .run(s"""
             | INSERT INTO $testTable VALUES
             | (TIMESTAMP '2023-10-01 05:00:00', 'F', 35, 'Vancouver')
             |""".stripMargin)
        .run(s"RECOVER INDEX JOB $testSkippingIndex")
        .assertIndexData(indexData => indexData should have size 6)
        .stopStreamingJob()
        .run(s"""
             | INSERT INTO $testTable VALUES
             | (TIMESTAMP '2023-10-01 06:00:00', 'G', 40, 'Vancouver')
             |""".stripMargin)
        .run(s"RECOVER INDEX JOB `$testSkippingIndex`") // test backtick name
        .assertIndexData(indexData => indexData should have size 7)

    }
  }

  test("recover covering index refresh job") {
    withFlintIndex(testCoveringIndex) { assertion =>
      assertion
        .run { checkpointDir =>
          s""" CREATE INDEX $testIndex ON $testTable
             | (time, name)
             | WITH (
             |   auto_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             |""".stripMargin
        }
        .assertIndexData(indexData => indexData should have size 5)
        .stopStreamingJob()
        .run(s"""
             | INSERT INTO $testTable VALUES
             | (TIMESTAMP '2023-10-01 05:00:00', 'F', 35, 'Vancouver')
             |""".stripMargin)
        .run(s"RECOVER INDEX JOB $testCoveringIndex")
        .assertIndexData(indexData => indexData should have size 6)
    }
  }

  test("recover materialized view refresh job") {
    withFlintIndex(testMvIndex) { assertion =>
      assertion
        .run { checkpointDir =>
          s""" CREATE MATERIALIZED VIEW $testMv
             | AS
             | $testMvQuery
             | WITH (
             |   auto_refresh = true,
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             |""".stripMargin
        }
        .assertIndexData(indexData => indexData should have size 5)
        .stopStreamingJob()
        .run(s"""
             | INSERT INTO $testTable VALUES
             | (TIMESTAMP '2023-10-01 05:00:00', 'F', 35, 'Vancouver')
             |""".stripMargin)
        .run(s"RECOVER INDEX JOB $testMvIndex")
        .assertIndexData(indexData => indexData should have size 6)
    }
  }

  private def withFlintIndex(flintIndexName: String)(test: AssertionHelper => Unit): Unit = {
    withTable(testTable) {
      createTimeSeriesTable(testTable)

      withTempDir { checkpointDir =>
        try {
          test(new AssertionHelper(flintIndexName, checkpointDir))
        } finally {
          deleteTestIndex(flintIndexName)
        }
      }
    }
  }

  /**
   * Recover test assertion helper that de-duplicates test code.
   */
  private class AssertionHelper(flintIndexName: String, checkpointDir: File) {

    def run(createIndex: File => String): AssertionHelper = {
      sql(createIndex(checkpointDir))
      this
    }

    def run(sqlText: String): AssertionHelper = {
      sql(sqlText)
      this
    }

    def assertIndexData(assertion: Array[Row] => Unit): AssertionHelper = {
      awaitStreamingComplete(findJobId(flintIndexName))
      assertion(flint.queryIndex(flintIndexName).collect())
      this
    }

    def stopStreamingJob(): AssertionHelper = {
      spark.streams.get(findJobId(flintIndexName)).stop()
      this
    }

    private def findJobId(indexName: String): String = {
      val job = spark.streams.active.find(_.name == indexName)
      job
        .map(_.id.toString)
        .getOrElse(throw new RuntimeException(s"Streaming job not found for index $indexName"))
    }
  }
}
