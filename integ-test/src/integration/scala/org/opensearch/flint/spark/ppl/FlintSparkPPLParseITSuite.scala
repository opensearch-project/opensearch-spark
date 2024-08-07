/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Sort}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

class FlintSparkPPLParseITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createPartitionedGrokEmailTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test parse email expressions parsing") {
    val frame = sql(s"""
         | source = $testTable | parse email '.+@(?<host>.+)' | fields email, host ;
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("Jake", 70), Row("Hello", 30), Row("John", 25), Row("Jane", 20))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    val fieldsProjectList = Seq(UnresolvedAttribute("name"), UnresolvedAttribute("age"))
    val evalProjectList = Seq(UnresolvedStar(None), Alias(Literal(1), "col")())
    val expectedPlan = Project(fieldsProjectList, Project(evalProjectList, table))
    // Compare the two plans
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

}
