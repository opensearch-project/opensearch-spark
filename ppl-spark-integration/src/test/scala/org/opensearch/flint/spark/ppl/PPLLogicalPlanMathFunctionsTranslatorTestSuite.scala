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
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanMathFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test abs") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = abs(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("abs", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test ceil") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = ceil(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("ceil", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test floor") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = floor(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("floor", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test ln") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = ln(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("ln", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test mod") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = mod(10, 4)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("mod", seq(Literal(10), Literal(4)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test pow") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = pow(2, 3)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("pow", seq(Literal(2), Literal(3)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test sqrt") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = sqrt(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("sqrt", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test arithmetic: + - * / %") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | where sqrt(pow(a, 2)) + sqrt(pow(a, 2)) / 1 - sqrt(pow(a, 2)) * 1 = sqrt(pow(a, 2)) % 1"),
        context)
    val table = UnresolvedRelation(Seq("t"))
    // sqrt(pow(a, 2))
    val sqrtPow =
      UnresolvedFunction(
        "sqrt",
        seq(
          UnresolvedFunction(
            "pow",
            seq(UnresolvedAttribute("a"), Literal(2)),
            isDistinct = false)),
        isDistinct = false)
    // sqrt(pow(a, 2)) / 1
    val sqrtPowDivide = UnresolvedFunction("/", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) * 1
    val sqrtPowMultiply =
      UnresolvedFunction("*", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) % 1
    val sqrtPowMod = UnresolvedFunction("%", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) + sqrt(pow(a, 2)) / 1
    val add = UnresolvedFunction("+", seq(sqrtPow, sqrtPowDivide), isDistinct = false)
    val sub = UnresolvedFunction("-", seq(add, sqrtPowMultiply), isDistinct = false)
    val filterExpr = EqualTo(sub, sqrtPowMod)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test boolean operators: = != < <= > >=") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = age = 30, b = age != 70, c = 30 < age, d = 30 <= age, e = 30 > age, f = 30 >= age | fields age, a, b, c, d, e, f"),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val evalProject = Project(
      Seq(
        UnresolvedStar(None),
        Alias(EqualTo(UnresolvedAttribute("age"), Literal(30)), "a")(),
        Alias(Not(EqualTo(UnresolvedAttribute("age"), Literal(70))), "b")(),
        Alias(LessThan(Literal(30), UnresolvedAttribute("age")), "c")(),
        Alias(LessThanOrEqual(Literal(30), UnresolvedAttribute("age")), "d")(),
        Alias(GreaterThan(Literal(30), UnresolvedAttribute("age")), "e")(),
        Alias(GreaterThanOrEqual(Literal(30), UnresolvedAttribute("age")), "f")()),
      table)
    val projectList = Seq(
      UnresolvedAttribute("age"),
      UnresolvedAttribute("a"),
      UnresolvedAttribute("b"),
      UnresolvedAttribute("c"),
      UnresolvedAttribute("d"),
      UnresolvedAttribute("e"),
      UnresolvedAttribute("f"))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test signum") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = signum(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("signum", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
}
