/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, CaseWhen, CurrentRow, Descending, LessThan, Literal, RowFrame, SortOrder, SpecifiedWindowFrame, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Project, Sort}

class PPLLogicalPlanTrendlineCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test trendline") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=relation | trendline sma(3, age)"), context)

    val table = UnresolvedRelation(Seq("relation"))
    val ageField = UnresolvedAttribute("age")
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val smaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(3)), Literal(null))), smaWindow)
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(caseWhen, "age_trendline")())
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, table))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("test trendline with sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort age sma(3, age)"),
        context)

    val table = UnresolvedRelation(Seq("relation"))
    val ageField = UnresolvedAttribute("age")
    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, table)
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val smaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(3)), Literal(null))), smaWindow)
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(caseWhen, "age_trendline")())
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sort))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("test trendline with sort and alias") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort - age sma(3, age) as age_sma"),
        context)

    val table = UnresolvedRelation(Seq("relation"))
    val ageField = UnresolvedAttribute("age")
    val sort = Sort(Seq(SortOrder(ageField, Descending)), global = true, table)
    val countWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val smaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val caseWhen = CaseWhen(Seq((LessThan(countWindow, Literal(3)), Literal(null))), smaWindow)
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(caseWhen, "age_sma")())
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sort))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("test trendline with multiple trendline sma commands") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | trendline sort + age sma(2, age) as two_points_sma sma(3, age) | fields name, age, two_points_sma, age_trendline"),
        context)

    val table = UnresolvedRelation(Seq("relation"))
    val nameField = UnresolvedAttribute("name")
    val ageField = UnresolvedAttribute("age")
    val ageTwoPointsSmaField = UnresolvedAttribute("two_points_sma")
    val ageTrendlineField = UnresolvedAttribute("age_trendline")
    val sort = Sort(Seq(SortOrder(ageField, Ascending)), global = true, table)
    val twoPointsCountWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
    val twoPointsSmaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-1), CurrentRow)))
    val threePointsCountWindow = new WindowExpression(
      UnresolvedFunction("COUNT", Seq(Literal(1)), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val threePointsSmaWindow = WindowExpression(
      UnresolvedFunction("AVG", Seq(ageField), isDistinct = false),
      WindowSpecDefinition(Seq(), Seq(), SpecifiedWindowFrame(RowFrame, Literal(-2), CurrentRow)))
    val twoPointsCaseWhen = CaseWhen(
      Seq((LessThan(twoPointsCountWindow, Literal(2)), Literal(null))),
      twoPointsSmaWindow)
    val threePointsCaseWhen = CaseWhen(
      Seq((LessThan(threePointsCountWindow, Literal(3)), Literal(null))),
      threePointsSmaWindow)
    val trendlineProjectList = Seq(
      UnresolvedStar(None),
      Alias(twoPointsCaseWhen, "two_points_sma")(),
      Alias(threePointsCaseWhen, "age_trendline")())
    val expectedPlan = Project(
      Seq(nameField, ageField, ageTwoPointsSmaField, ageTrendlineField),
      Project(trendlineProjectList, sort))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }
}
