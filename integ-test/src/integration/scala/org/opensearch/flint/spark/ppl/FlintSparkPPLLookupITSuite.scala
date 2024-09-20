/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLLookupITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createOccupationTable(testTable1)
    createWorkInformationTable(testTable2)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test(
    "test LookupCriteria -> ON, Single Condition, OutputStrategy -> REPLACE, Single OutputField") {
    val frame = sql(s"""
                       | source = $testTable1 | LOOKUP $testTable2 firstname AS name REPLACE newSalary AS salary
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    results.foreach(println(_))
  }

  test(
    "test LookupCriteria -> ON, Multiple Conditions, OutputStrategy -> REPLACE, Single OutputField") {
    val frame = sql(s"""
                       | source = $testTable1 | LOOKUP $testTable2 ON name = firstname AND $testTable1.salary = $testTable2.salary REPLACE newSalary AS salary
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    results.foreach(println(_))
  }

  test("test jon") {
    val frame = sql(s"""
                       | source = $testTable1 | join left=l right=r ON name = firstname AND l.salary = r.salary $testTable2
                       | """.stripMargin)
    val results: Array[Row] = frame.collect()
    results.foreach(println(_))
  }
}
