/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{QueryTest, Row}

class FlintSparkPPLTrendlineITSuite
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

  test("trendline sma") {
    val frame = sql(s"""
                       | source = $testTable | trendline sma(2, age) as first_age_sma sma(3, age) as second_age_sma | fields name, first_age_sma, second_age_sma
                       | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array()

    // Convert actual results to a Set for quick lookup
    val resultsSet: Set[Row] = results.toSet
    // Check that each expected row is present in the actual results
    expectedResults.foreach { expectedRow =>
      assert(resultsSet.contains(expectedRow), s"Expected row $expectedRow not found in results")
    }
    // Retrieve the logical plan
    val logicalPlan: LogicalPlan =
      frame.queryExecution.commandExecuted.asInstanceOf[CommandResult].commandLogicalPlan
    // Define the expected logical plan
    val expectedPlan: LogicalPlan = Project(Seq(), new UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test")))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
