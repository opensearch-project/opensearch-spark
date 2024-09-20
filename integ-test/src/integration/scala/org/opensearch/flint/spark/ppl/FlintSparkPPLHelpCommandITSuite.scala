/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.PrintLiteralCommandDescriptionLogicalPlan
import org.opensearch.sql.ppl.parser.AstCommandDescriptionVisitor

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLHelpCommandITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("search -help command") {
    val helpText =
      """
        |SEARCH Command:
        |
        |Syntax:
        |   (SEARCH)? fromClause
        |   | (SEARCH)? fromClause logicalExpression
        |   | (SEARCH)? logicalExpression fromClause
        |
        |Description:
        |The SEARCH command is used to retrieve data from a specified source. It can be used with or without additional filters.
        |- You can specify the data source using the FROM clause.
        |- You can add filters using logical expressions.
        |- The order of FROM clause and logical expression can be interchanged.
""".stripMargin.trim

    val pplParser = new PPLSyntaxParser()
    val frame = sql(s"""
           search -help
           """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(AstCommandDescriptionVisitor.describeCommand(helpText, pplParser.getParserVersion())))

    // Extract values from rows, assuming single column
    val actualValues = results.map(_.getAs[String](0))
    val expectedValues = expectedResults.map(_.getAs[String](0))

    assert(
      actualValues.sameElements(expectedValues),
      s"""
         |Expected: ${expectedValues.mkString(", ")}
         |Actual: ${actualValues.mkString(", ")}
         |""".stripMargin)

    // Retrieve the logical plan
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    // Expected plan should match the produced custom logical plan
    val expectedPlan: LogicalPlan = PrintLiteralCommandDescriptionLogicalPlan(
      AstCommandDescriptionVisitor.describeCommand(helpText, pplParser.getParserVersion()))

    // Compare the plans
    comparePlans(expectedPlan, logicalPlan, false)
  }

}
