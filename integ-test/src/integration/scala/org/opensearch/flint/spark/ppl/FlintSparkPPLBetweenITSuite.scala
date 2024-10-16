/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLBetweenITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test between should return records between two values") {
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

  test("test between should return records NOT between two values") {
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
}
