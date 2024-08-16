/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Descending, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.execution.command.DescribeTableCommand

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
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=accounts | rare address", false), context)
    val addressField = UnresolvedAttribute("address")
    val tableRelation = UnresolvedRelation(Seq("accounts"))

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
        "count(address)")(),
      addressField)

    val aggregatePlan =
      Aggregate(Seq(addressField), aggregateExpressions, tableRelation)

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("address"), Descending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }


  test("test simple rare command with a by field test") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logicalPlan =
      planTransformer.visit(plan(pplParser, "source=accounts | rare address by age", false), context)
    // Retrieve the logical plan
    // Define the expected logical plan
    val addressField = UnresolvedAttribute("address")
    val ageField = UnresolvedAttribute("age")
    val ageAlias = Alias(ageField, "age")()

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val countExpr = Alias(UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false), "count(address)")()

    val aggregateExpressions = Seq(
      countExpr,
      addressField,
      ageAlias)
    val aggregatePlan =
      Aggregate(
        Seq(addressField, ageAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("accounts")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("address"), Descending)),
        global = true,
        aggregatePlan)

    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logicalPlan, false)
  }

  test("test simple top command with a single field") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=accounts | top address", false), context)
    val addressField = UnresolvedAttribute("address")
    val tableRelation = UnresolvedRelation(Seq("accounts"))

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))

    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false),
        "count(address)")(),
      addressField)

    val aggregatePlan =
      Aggregate(Seq(addressField), aggregateExpressions, tableRelation)

    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("address"), Ascending)), global = true, aggregatePlan)
    val expectedPlan = Project(projectList, sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple top 1 command by age field") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=accounts | top 1 address by age", false), context)

    val addressField = UnresolvedAttribute("address")
    val ageField = UnresolvedAttribute("age")
    val ageAlias = Alias(ageField, "age")()

    val countExpr = Alias(UnresolvedFunction(Seq("COUNT"), Seq(addressField), isDistinct = false), "count(address)")()
    val aggregateExpressions = Seq(
      countExpr,
      addressField,
      ageAlias)
    val aggregatePlan =
      Aggregate(
        Seq(addressField, ageAlias),
        aggregateExpressions,
        UnresolvedRelation(Seq("accounts")))

    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("address"), Ascending)),
        global = true,
        aggregatePlan)

    val planWithLimit =
      GlobalLimit(Literal(1), LocalLimit(Literal(1), sortedPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), planWithLimit)
    comparePlans(expectedPlan, logPlan, false)
  }
}
