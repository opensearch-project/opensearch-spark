/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLLookupITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  private val lookupTable = "spark_catalog.default.flint_ppl_test_lookup"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedStateCountryTable(testTable)
    createOccupationTable(lookupTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create ppl simple query test") {
    val frame = sql(s"""
         | source = $testTable | where age > 20 | lookup flint_ppl_test_lookup name
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()

    assert(results.length == 3)

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("Jake", 70, "California", "USA", 2023, 4, "Jake", "Engineer", "England", 100000, 2023, 4),
      Row("Hello", 30, "New York", "USA", 2023, 4, "Hello", "Artist", "USA", 70000, 2023, 4),
      Row("John", 25, "Ontario", "Canada", 2023, 4, "John", "Doctor", "Canada", 120000, 2023, 4))
    // Compare the results
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val expectedPlan: LogicalPlan =
      Project(
        Seq(UnresolvedStar(None)),
        Join(
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")),
          UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")),
          JoinType.apply("left"),
          Option.empty,
          JoinHint.NONE
        )
        //UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
      )
    // Compare the two plans
    assert(expectedPlan === logicalPlan)
  }
}


