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
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not, Or}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanParenthesizedConditionTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test simple nested condition") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE (age > 18 AND (state = 'California' OR state = 'New York'))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        GreaterThan(UnresolvedAttribute("age"), Literal(18)),
        Or(
          EqualTo(UnresolvedAttribute("state"), Literal("California")),
          EqualTo(UnresolvedAttribute("state"), Literal("New York")))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test nested condition with duplicated parentheses") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE ((((age > 18) AND ((((state = 'California') OR state = 'New York'))))))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        GreaterThan(UnresolvedAttribute("age"), Literal(18)),
        Or(
          EqualTo(UnresolvedAttribute("state"), Literal("California")),
          EqualTo(UnresolvedAttribute("state"), Literal("New York")))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test combining between function") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE (year = 2023 AND (month BETWEEN 1 AND 6)) AND (age >= 31 OR country = 'Canada')"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val betweenCondition = And(
      GreaterThanOrEqual(UnresolvedAttribute("month"), Literal(1)),
      LessThanOrEqual(UnresolvedAttribute("month"), Literal(6)))
    val filter = Filter(
      And(
        And(EqualTo(UnresolvedAttribute("year"), Literal(2023)), betweenCondition),
        Or(
          GreaterThanOrEqual(UnresolvedAttribute("age"), Literal(31)),
          EqualTo(UnresolvedAttribute("country"), Literal("Canada")))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test multiple levels of nesting") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE ((state = 'Texas' OR state = 'California') AND (age < 30 OR (country = 'USA' AND year > 2020)))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        Or(
          EqualTo(UnresolvedAttribute("state"), Literal("Texas")),
          EqualTo(UnresolvedAttribute("state"), Literal("California"))),
        Or(
          LessThan(UnresolvedAttribute("age"), Literal(30)),
          And(
            EqualTo(UnresolvedAttribute("country"), Literal("USA")),
            GreaterThan(UnresolvedAttribute("year"), Literal(2020))))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test with string functions") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE (LIKE(LOWER(name), 'a%') OR LIKE(LOWER(name), 'j%')) AND (LENGTH(state) > 6 OR (country = 'USA' AND age > 18))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        Or(
          UnresolvedFunction(
            "like",
            Seq(
              UnresolvedFunction("lower", Seq(UnresolvedAttribute("name")), isDistinct = false),
              Literal("a%")),
            isDistinct = false),
          UnresolvedFunction(
            "like",
            Seq(
              UnresolvedFunction("lower", Seq(UnresolvedAttribute("name")), isDistinct = false),
              Literal("j%")),
            isDistinct = false)),
        Or(
          GreaterThan(
            UnresolvedFunction("length", Seq(UnresolvedAttribute("state")), isDistinct = false),
            Literal(6)),
          And(
            EqualTo(UnresolvedAttribute("country"), Literal("USA")),
            GreaterThan(UnresolvedAttribute("age"), Literal(18))))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test complex age ranges with nested conditions") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE (age BETWEEN 25 AND 40) AND ((state IN ('California', 'New York', 'Texas') AND year = 2023) OR (country != 'USA' AND (month = 1 OR month = 12)))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        And(
          GreaterThanOrEqual(UnresolvedAttribute("age"), Literal(25)),
          LessThanOrEqual(UnresolvedAttribute("age"), Literal(40))),
        Or(
          And(
            In(
              UnresolvedAttribute("state"),
              Seq(Literal("California"), Literal("New York"), Literal("Texas"))),
            EqualTo(UnresolvedAttribute("year"), Literal(2023))),
          And(
            Not(EqualTo(UnresolvedAttribute("country"), Literal("USA"))),
            Or(
              EqualTo(UnresolvedAttribute("month"), Literal(1)),
              EqualTo(UnresolvedAttribute("month"), Literal(12)))))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test nested NOT conditions") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE NOT (age < 18 OR (state = 'Alaska' AND year < 2020)) AND (country = 'USA' OR (country = 'Mexico' AND month BETWEEN 6 AND 8))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        Not(
          Or(
            LessThan(UnresolvedAttribute("age"), Literal(18)),
            And(
              EqualTo(UnresolvedAttribute("state"), Literal("Alaska")),
              LessThan(UnresolvedAttribute("year"), Literal(2020))))),
        Or(
          EqualTo(UnresolvedAttribute("country"), Literal("USA")),
          And(
            EqualTo(UnresolvedAttribute("country"), Literal("Mexico")),
            And(
              GreaterThanOrEqual(UnresolvedAttribute("month"), Literal(6)),
              LessThanOrEqual(UnresolvedAttribute("month"), Literal(8)))))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test complex boolean logic") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | WHERE (NOT (year < 2020 OR age < 18)) AND ((state = 'Texas' AND month % 2 = 0) OR (country = 'Mexico' AND (year = 2023 OR (year = 2022 AND month > 6))))"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter = Filter(
      And(
        Not(
          Or(
            LessThan(UnresolvedAttribute("year"), Literal(2020)),
            LessThan(UnresolvedAttribute("age"), Literal(18)))),
        Or(
          And(
            EqualTo(UnresolvedAttribute("state"), Literal("Texas")),
            EqualTo(
              UnresolvedFunction(
                "%",
                Seq(UnresolvedAttribute("month"), Literal(2)),
                isDistinct = false),
              Literal(0))),
          And(
            EqualTo(UnresolvedAttribute("country"), Literal("Mexico")),
            Or(
              EqualTo(UnresolvedAttribute("year"), Literal(2023)),
              And(
                EqualTo(UnresolvedAttribute("year"), Literal(2022)),
                GreaterThan(UnresolvedAttribute("month"), Literal(6))))))),
      table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
