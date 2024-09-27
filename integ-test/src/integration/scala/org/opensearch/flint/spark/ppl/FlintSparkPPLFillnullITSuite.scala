/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFillnullITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createNullableTableHttpLog(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test fillnull with one null replacement value and one column") {
    val frame = sql(s"""
                       | source = $testTable | fillnull value = 0 status_code
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", null, 200),
        Row(2, "/about", "2023-10-01 10:05:00", 0),
        Row(3, "/contact", "2023-10-01 10:10:00", 0),
        Row(4, null, "2023-10-01 10:15:00", 301),
        Row(5, null, "2023-10-01 10:20:00", 200),
        Row(6, "/home", null, 403))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test fillnull with various null replacement values and one column") {
    val frame = sql(s"""
                       | source = $testTable | fillnull fields status_code=101
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", null, 200),
        Row(2, "/about", "2023-10-01 10:05:00", 101),
        Row(3, "/contact", "2023-10-01 10:10:00", 101),
        Row(4, null, "2023-10-01 10:15:00", 301),
        Row(5, null, "2023-10-01 10:20:00", 200),
        Row(6, "/home", null, 403))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test fillnull with one null replacement value and two columns") {
    val frame = sql(s"""
                       | source = $testTable | fillnull value = '???' request_path, timestamp | fields id, request_path, timestamp
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", "???"),
        Row(2, "/about", "2023-10-01 10:05:00"),
        Row(3, "/contact", "2023-10-01 10:10:00"),
        Row(4, "???", "2023-10-01 10:15:00"),
        Row(5, "???", "2023-10-01 10:20:00"),
        Row(6, "/home", "???"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test fillnull with various null replacement values and two columns") {
    val frame = sql(s"""
                       | source = $testTable | fillnull fields request_path='/not_found', timestamp='*' | fields id, request_path, timestamp
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "/home", "*"),
        Row(2, "/about", "2023-10-01 10:05:00"),
        Row(3, "/contact", "2023-10-01 10:10:00"),
        Row(4, "/not_found", "2023-10-01 10:15:00"),
        Row(5, "/not_found", "2023-10-01 10:20:00"),
        Row(6, "/home", "*"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }
}
