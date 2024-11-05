/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.sql.Timestamp

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLBetweenITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val timeSeriesTestTable = "spark_catalog.default.flint_ppl_timeseries_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test tables
    createPartitionedStateCountryTable(testTable)
    createTimeSeriesTable(timeSeriesTestTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test between should return records between two integer values") {
    val frame = sql(s"""
                       | source = $testTable | where age between 20 and 30
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 3)
    assert(frame.columns.length == 6)

    results.foreach(row => {
      val age = row.getAs[Int]("age")
      assert(age >= 20 && age <= 30, s"Age $age is not between 20 and 30")
    })
  }

  test("test between should return records between two integer computed values") {
    val frame = sql(s"""
                       | source = $testTable | where age between 20 + 1 and 30 - 1
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 1)
    assert(frame.columns.length == 6)

    results.foreach(row => {
      val age = row.getAs[Int]("age")
      assert(age >= 21 && age <= 29, s"Age $age is not between 21 and 29")
    })
  }

  test("test between should return records NOT between two integer values") {
    val frame = sql(s"""
                       | source = $testTable | where age NOT between 20 and 30
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 1)
    assert(frame.columns.length == 6)

    results.foreach(row => {
      val age = row.getAs[Int]("age")
      assert(age < 20 || age > 30, s"Age $age is not between 20 and 30")
    })
  }

  test("test between should return records where NOT between two integer values") {
    val frame = sql(s"""
                       | source = $testTable | where NOT age between 20 and 30
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 1)
    assert(frame.columns.length == 6)

    results.foreach(row => {
      val age = row.getAs[Int]("age")
      assert(age < 20 || age > 30, s"Age $age is not between 20 and 30")
    })
  }

  test("test between should return records between two date values") {
    val frame = sql(s"""
                       | source = $timeSeriesTestTable | where time between '2023-10-01 00:01:00' and '2023-10-01 00:10:00'
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 2)
    assert(frame.columns.length == 4)

    results.foreach(row => {
      val ts = row.getAs[Timestamp]("time")
      assert(
        !ts.before(Timestamp.valueOf("2023-10-01 00:01:00")) || !ts.after(
          Timestamp.valueOf("2023-10-01 00:01:00")),
        s"Timestamp $ts is not between '2023-10-01 00:01:00' and '2023-10-01 00:10:00'")
    })
  }

  test("test between should return records NOT between two date values") {
    val frame = sql(s"""
                       | source = $timeSeriesTestTable | where time NOT between '2023-10-01 00:01:00' and '2023-10-01 00:10:00'
                       | """.stripMargin)

    val results = frame.collect()
    assert(results.length == 3)
    assert(frame.columns.length == 4)

    results.foreach(row => {
      val ts = row.getAs[Timestamp]("time")
      assert(
        ts.before(Timestamp.valueOf("2023-10-01 00:01:00")) || ts.after(
          Timestamp.valueOf("2023-10-01 00:01:00")),
        s"Timestamp $ts is not between '2023-10-01 00:01:00' and '2023-10-01 00:10:00'")
    })

  }
}
