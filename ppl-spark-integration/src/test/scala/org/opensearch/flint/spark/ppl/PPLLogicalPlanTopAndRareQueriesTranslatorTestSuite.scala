/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.DescribeTableCommand
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanTopAndRareQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  
  test("test simple rare command with a single field") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=accounts | rare gender", false), context)
    val genderField = UnresolvedAttribute("gender")
    val tableRelation = UnresolvedRelation(Seq("accounts"))

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(genderField), isDistinct = false), "count(gender)")(),
      genderField
    )

    val aggregatePlan =
      Aggregate(Seq(genderField), aggregateExpressions, tableRelation)

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("gender"), Descending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
  
  test("test simple top command with a single field") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=accounts | top gender", false), context)
    val genderField = UnresolvedAttribute("gender")
    val tableRelation = UnresolvedRelation(Seq("accounts"))

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(UnresolvedFunction(Seq("COUNT"), Seq(genderField), isDistinct = false), "count(gender)")(),
      genderField
    )

    val aggregatePlan =
      Aggregate(Seq(genderField), aggregateExpressions, tableRelation)

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("gender"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
}
