/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanMathFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test abs") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = abs(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("abs", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test ceil") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = ceil(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("ceil", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test floor") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = floor(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("floor", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test ln") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = ln(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("ln", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test mod") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = mod(10, 4)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("mod", seq(Literal(10), Literal(4)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test pow") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = pow(2, 3)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("pow", seq(Literal(2), Literal(3)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test sqrt") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = sqrt(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("sqrt", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test arithmetic: + - * / %") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | where sqrt(pow(a, 2)) + sqrt(pow(a, 2)) / 1 - sqrt(pow(a, 2)) * 1 = sqrt(pow(a, 2)) % 1",
          false),
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
    val sqrtPowDivide = UnresolvedFunction("divide", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) * 1
    val sqrtPowMultiply =
      UnresolvedFunction("multiply", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) % 1
    val sqrtPowMod = UnresolvedFunction("modulus", seq(sqrtPow, Literal(1)), isDistinct = false)
    // sqrt(pow(a, 2)) + sqrt(pow(a, 2)) / 1
    val add = UnresolvedFunction("add", seq(sqrtPow, sqrtPowDivide), isDistinct = false)
    val sub = UnresolvedFunction("subtract", seq(add, sqrtPowMultiply), isDistinct = false)
    val filterExpr = EqualTo(sub, sqrtPowMod)
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }
}
