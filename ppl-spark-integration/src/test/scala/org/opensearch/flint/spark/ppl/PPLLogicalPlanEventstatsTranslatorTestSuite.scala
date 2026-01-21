/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Divide, Floor, Literal, Multiply, RowFrame, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Window}

class PPLLogicalPlanEventstatsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test eventstats avg") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source = table | eventstats avg(age)"), context)

    val table = UnresolvedRelation(Seq("table"))
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg(age)")()
    val windowPlan = Window(Seq(avgWindowExprAlias), Nil, Nil, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test eventstats avg, max, min, count") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count"),
        context)

    val table = UnresolvedRelation(Seq("table"))
    val avgWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(avgWindowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        Nil,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      Nil,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eventstats avg by country") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source = table | eventstats avg(age) by country"),
        context)

    val table = UnresolvedRelation(Seq("table"))
    val partitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg(age)")()
    val windowPlan = Window(Seq(avgWindowExprAlias), partitionSpec, Nil, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count by country") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by country"),
        context)

    val table = UnresolvedRelation(Seq("table"))
    val partitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val avgWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(avgWindowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      partitionSpec,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eventstats avg, max, min, count by span") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | eventstats avg(age) as avg_age, max(age) as max_age, min(age) as min_age, count(age) as count by span(age, 10) as age_span"),
        context)

    val table = UnresolvedRelation(Seq("table"))
    val span = Alias(
      Multiply(Floor(Divide(UnresolvedAttribute("age"), Literal(10))), Literal(10)),
      "age_span")()
    val partitionSpec = Seq(span)
    val windowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgWindowExprAlias = Alias(windowExpression, "avg_age")()

    val maxWindowExpression = WindowExpression(
      UnresolvedFunction("MAX", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val maxWindowExprAlias = Alias(maxWindowExpression, "max_age")()

    val minWindowExpression = WindowExpression(
      UnresolvedFunction("MIN", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val minWindowExprAlias = Alias(minWindowExpression, "min_age")()

    val countWindowExpression = WindowExpression(
      UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val countWindowExprAlias = Alias(countWindowExpression, "count")()
    val windowPlan = Window(
      Seq(avgWindowExprAlias, maxWindowExprAlias, minWindowExprAlias, countWindowExprAlias),
      partitionSpec,
      Nil,
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test multiple eventstats") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | eventstats avg(age) as avg_age by state, country | eventstats avg(avg_age) as avg_state_age by country"),
        context)

    val partitionSpec = Seq(
      Alias(UnresolvedAttribute("state"), "state")(),
      Alias(UnresolvedAttribute("country"), "country")())
    val table = UnresolvedRelation(Seq("table"))
    val avgAgeWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("age")), isDistinct = false),
      WindowSpecDefinition(
        partitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgAgeWindowExprAlias = Alias(avgAgeWindowExpression, "avg_age")()
    val windowPlan1 = Window(Seq(avgAgeWindowExprAlias), partitionSpec, Nil, table)

    val countryPartitionSpec = Seq(Alias(UnresolvedAttribute("country"), "country")())
    val avgStateAgeWindowExpression = WindowExpression(
      UnresolvedFunction("AVG", Seq(UnresolvedAttribute("avg_age")), isDistinct = false),
      WindowSpecDefinition(
        countryPartitionSpec,
        Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)))
    val avgStateAgeWindowExprAlias = Alias(avgStateAgeWindowExpression, "avg_state_age")()
    val windowPlan2 =
      Window(Seq(avgStateAgeWindowExprAlias), countryPartitionSpec, Nil, windowPlan1)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), windowPlan2)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
