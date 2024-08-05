/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanTimeFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test from_unixtime") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = from_unixtime(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("from_unixtime", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test unix_timestamp") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = unix_timestamp(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("unix_timestamp", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test builtin time functions with name mapping") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
           | source = t
           | | eval a = DAY_OF_WEEK(DATE('2020-08-26'))
           | | eval b = DAY_OF_MONTH(DATE('2020-08-26'))
           | | eval c = DAY_OF_YEAR(DATE('2020-08-26'))
           | | eval d = WEEK_OF_YEAR(DATE('2020-08-26'))
           | | eval e = WEEK(DATE('2020-08-26'))
           | | eval f = MONTH_OF_YEAR(DATE('2020-08-26'))
           | | eval g = HOUR_OF_DAY(DATE('2020-08-26'))
           | | eval h = MINUTE_OF_HOUR(DATE('2020-08-26'))
           | | eval i = SECOND_OF_MINUTE(DATE('2020-08-26'))
           | | eval j = SUBDATE(DATE('2020-08-26'), 1)
           | | eval k = ADDDATE(DATE('2020-08-26'), 1)
           | | eval l = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59'))
           | | eval m = LOCALTIME()
           | """.stripMargin,
          false),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val projectA = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "dayofweek",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "a")()),
      table)
    val projectB = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "dayofmonth",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "b")()),
      projectA)
    val projectC = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "dayofyear",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "c")()),
      projectB)
    val projectD = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "weekofyear",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "d")()),
      projectC)
    val projectE = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "weekofyear",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "e")()),
      projectD)
    val projectF = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "month",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "f")()),
      projectE)
    val projectG = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "hour",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "g")()),
      projectF)
    val projectH = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "minute",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "h")()),
      projectG)
    val projectI = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "second",
            Seq(UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false)),
            isDistinct = false),
          "i")()),
      projectH)
    val projectJ = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "date_sub",
            Seq(
              UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false),
              Literal(1)),
            isDistinct = false),
          "j")()),
      projectI)
    val projectK = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "date_add",
            Seq(
              UnresolvedFunction("date", Seq(Literal("2020-08-26")), isDistinct = false),
              Literal(1)),
            isDistinct = false),
          "k")()),
      projectJ)
    val projectL = Project(
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "datediff",
            Seq(
              UnresolvedFunction(
                "timestamp",
                Seq(Literal("2000-01-02 00:00:00")),
                isDistinct = false),
              UnresolvedFunction(
                "timestamp",
                Seq(Literal("2000-01-01 23:59:59")),
                isDistinct = false)),
            isDistinct = false),
          "l")()),
      projectK)
    val projectM = Project(
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedFunction("localtimestamp", Seq.empty, isDistinct = false), "m")()),
      projectL)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, projectM)
    comparePlans(expectedPlan, logPlan, false)
  }
}
