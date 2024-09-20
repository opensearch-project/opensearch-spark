/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.PrintLiteralCommandDescriptionLogicalPlan
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.parser.AstCommandDescriptionVisitor
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, Descending, GenericInternalRow, GreaterThan, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class PPLLogicalPlanHelpCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test help search command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "search -help", false), context)

    val helpText = """
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
    // Expected plan should match the produced custom logical plan
    val expectedPlan: LogicalPlan = PrintLiteralCommandDescriptionLogicalPlan(
      AstCommandDescriptionVisitor.describeCommand(helpText, pplParser.getParserVersion()))

    // Compare the plans
    comparePlans(expectedPlan, logPlan, false)
  }
}
