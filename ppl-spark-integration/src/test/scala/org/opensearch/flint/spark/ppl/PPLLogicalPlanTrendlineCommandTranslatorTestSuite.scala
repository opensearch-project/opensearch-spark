/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.opensearch.sql.ppl.utils.SortUtils
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Ascending, CaseWhen, CurrentRow, Descending, Divide, Expression, LessThan, Literal, Multiply, RowFrame, SortOrder, SpecifiedWindowFrame, WindowExpression, WindowSpecDefinition}
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

  test("test trendline with sort and backticks alias") {
    val expectedPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort - age sma(3, age) as age_sma"),
        new CatalystPlanContext)
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort - `age` sma(3, `age`) as `age_sma`"),
        new CatalystPlanContext)
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

  test("WMA - with sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort age wma(3, age)"),
        context)

    val dividend = Add(
      Add(
        getNthValueAggregation("age", "age", 1, -2),
        getNthValueAggregation("age", "age", 2, -2)),
      getNthValueAggregation("age", "age", 3, -2))
    val wmaExpression = Divide(dividend, Literal(6))
    val trendlineProjectList = Seq(UnresolvedStar(None), Alias(wmaExpression, "age_trendline")())
    val sortedTable = Sort(
      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
      global = true,
      UnresolvedRelation(Seq("relation")))
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))

    /**
     * Expected logical plan: 'Project [*] !+- 'Project [*, ((( ('nth_value('age, 1)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
     * currentrow$())) * 1) + ('nth_value('age, 2) windowspecdefinition('age ASC NULLS FIRST,
     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 2)) + ('nth_value('age, 3)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
     * currentrow$())) * 3)) / 6) AS age_trendline#0] ! +- 'Sort ['age ASC NULLS FIRST], true ! +-
     * 'UnresolvedRelation [relation], [], false
     */
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("WMA - with sort and alias") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=relation | trendline sort age wma(3, age) as TEST_CUSTOM_COLUMN"),
        context)

    val dividend = Add(
      Add(
        getNthValueAggregation("age", "age", 1, -2),
        getNthValueAggregation("age", "age", 2, -2)),
      getNthValueAggregation("age", "age", 3, -2))
    val wmaExpression = Divide(dividend, Literal(6))
    val trendlineProjectList =
      Seq(UnresolvedStar(None), Alias(wmaExpression, "TEST_CUSTOM_COLUMN")())
    val sortedTable = Sort(
      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
      global = true,
      UnresolvedRelation(Seq("relation")))

    /**
     * Expected logical plan: 'Project [*] !+- 'Project [*, ((( ('nth_value('age, 1)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
     * currentrow$())) * 1) + ('nth_value('age, 2) windowspecdefinition('age ASC NULLS FIRST,
     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 2)) + ('nth_value('age, 3)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
     * currentrow$())) * 3)) / 6) AS TEST_CUSTOM_COLUMN#0] ! +- 'Sort ['age ASC NULLS FIRST], true
     * ! +- 'UnresolvedRelation [relation], [], false
     */
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)

  }

  test("WMA - multiple trendline commands") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=relation | trendline sort age wma(2, age) as two_points_wma wma(3, age) as three_points_wma"),
        context)

    val dividendTwo = Add(
      getNthValueAggregation("age", "age", 1, -1),
      getNthValueAggregation("age", "age", 2, -1))
    val twoPointsExpression = Divide(dividendTwo, Literal(3))

    val dividend = Add(
      Add(
        getNthValueAggregation("age", "age", 1, -2),
        getNthValueAggregation("age", "age", 2, -2)),
      getNthValueAggregation("age", "age", 3, -2))
    val threePointsExpression = Divide(dividend, Literal(6))
    val trendlineProjectList = Seq(
      UnresolvedStar(None),
      Alias(twoPointsExpression, "two_points_wma")(),
      Alias(threePointsExpression, "three_points_wma")())
    val sortedTable = Sort(
      Seq(SortOrder(UnresolvedAttribute("age"), Ascending)),
      global = true,
      UnresolvedRelation(Seq("relation")))

    /**
     * Expected logical plan: 'Project [*] +- 'Project [*, (( ('nth_value('age, 1)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -1,
     * currentrow$())) * 1) + ('nth_value('age, 2) windowspecdefinition('age ASC NULLS FIRST,
     * specifiedwindowframe(RowFrame, -1, currentrow$())) * 2)) / 3) AS two_points_wma#0,
     *
     * ((( ('nth_value('age, 1) windowspecdefinition('age ASC NULLS FIRST,
     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 1) + ('nth_value('age, 2)
     * windowspecdefinition('age ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2,
     * currentrow$())) * 2)) + ('nth_value('age, 3) windowspecdefinition('age ASC NULLS FIRST,
     * specifiedwindowframe(RowFrame, -2, currentrow$())) * 3)) / 6) AS three_points_wma#1] +-
     * 'Sort ['age ASC NULLS FIRST], true +- 'UnresolvedRelation [relation], [], false
     */
    val expectedPlan =
      Project(Seq(UnresolvedStar(None)), Project(trendlineProjectList, sortedTable))
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)

  }

  test("WMA - with negative dataPoint value") {
    val context = new CatalystPlanContext
    val exception = intercept[SyntaxCheckException](
      planTransformer
        .visit(plan(pplParser, "source=relation | trendline sort age wma(-3, age)"), context))
    assert(exception.getMessage startsWith "Failed to parse query due to offending symbol [-]")
  }

  private def getNthValueAggregation(
      dataField: String,
      sortField: String,
      lookBackPos: Int,
      lookBackRange: Int): Expression = {
    Multiply(
      WindowExpression(
        UnresolvedFunction(
          "nth_value",
          Seq(UnresolvedAttribute(dataField), Literal(lookBackPos)),
          isDistinct = false),
        WindowSpecDefinition(
          Seq(),
          seq(SortUtils.sortOrder(UnresolvedAttribute(sortField), true)),
          SpecifiedWindowFrame(RowFrame, Literal(lookBackRange), CurrentRow))),
      Literal(lookBackPos))
  }

}
