/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentTimestamp, CurrentTimeZone, DateAddInterval, Literal, MakeInterval, NamedExpression, TimestampAdd, TimestampDiff, ToUTCTimestamp, UnaryMinus}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project

class PPLLogicalPlanDateTimeFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test DATE_ADD") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a =  DATE_ADD(DATE('2020-08-26'), INTERVAL 2 DAY)"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        DateAddInterval(
          UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false),
          MakeInterval(
            Literal(0),
            Literal(0),
            Literal(0),
            Literal(2),
            Literal(0),
            Literal(0),
            Literal(0),
            failOnError = true)),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test DATE_ADD for year") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a =  DATE_ADD(DATE('2020-08-26'), INTERVAL 2 YEAR)"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        DateAddInterval(
          UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false),
          MakeInterval(
            Literal(2),
            Literal(0),
            Literal(0),
            Literal(0),
            Literal(0),
            Literal(0),
            Literal(0),
            failOnError = true)),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test DATE_SUB") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a =  DATE_SUB(DATE('2020-08-26'), INTERVAL 2 DAY)"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        DateAddInterval(
          UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false),
          UnaryMinus(
            MakeInterval(
              Literal(0),
              Literal(0),
              Literal(0),
              Literal(2),
              Literal(0),
              Literal(0),
              Literal(0),
              failOnError = true),
            failOnError = true)),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test TIMESTAMPADD") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(TimestampAdd("DAY", Literal(17), Literal("2000-01-01 00:00:00")), "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test TIMESTAMPADD with timestamp") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = TIMESTAMPADD(DAY, 17, TIMESTAMP('2000-01-01 00:00:00'))"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        TimestampAdd(
          "DAY",
          Literal(17),
          UnresolvedFunction(
            "timestamp",
            Seq(Literal("2000-01-01 00:00:00")),
            isDistinct = false)),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test TIMESTAMPDIFF") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        TimestampDiff("YEAR", Literal("1997-01-01 00:00:00"), Literal("2001-03-06 00:00:00")),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test TIMESTAMPDIFF with timestamp") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = TIMESTAMPDIFF(YEAR, TIMESTAMP('1997-01-01 00:00:00'), TIMESTAMP('2001-03-06 00:00:00'))"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        TimestampDiff(
          "YEAR",
          UnresolvedFunction(
            "timestamp",
            Seq(Literal("1997-01-01 00:00:00")),
            isDistinct = false),
          UnresolvedFunction(
            "timestamp",
            Seq(Literal("2001-03-06 00:00:00")),
            isDistinct = false)),
        "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test UTC_TIMESTAMP") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = UTC_TIMESTAMP()"), context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(ToUTCTimestamp(CurrentTimestamp(), CurrentTimeZone()), "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test CURRENT_TIMEZONE") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = CURRENT_TIMEZONE()"), context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("current_timezone", Seq.empty, isDistinct = false), "a")())
    val eval = Project(evalProjectList, table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), eval)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
