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
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Like, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanStringFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test unknown function") {
    val context = new CatalystPlanContext
    intercept[SyntaxCheckException] {
      planTransformer.visit(plan(pplParser, "source=t a = unknown(b)", false), context)
    }
  }

  test("test concat") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=t a = CONCAT('hello', 'world')", false),
      context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("concat", seq(Literal("hello"), Literal("world")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test concat with field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = CONCAT('hello', b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction(
        "concat",
        seq(Literal("hello"), UnresolvedAttribute("b")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test length") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = LENGTH(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("length", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test lower") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = LOWER(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("lower", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test upper - case insensitive") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = uPPer(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("upper", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test trim") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = trim(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("trim", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test ltrim") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = ltrim(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("ltrim", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test rtrim") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = rtrim(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("rtrim", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test substring") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = substring(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("substring", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test like") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a=like(b, 'Hatti_')", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val likeExpr = new Like(UnresolvedAttribute("a"), Literal("Hatti_"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction(
        "like",
        seq(UnresolvedAttribute("b"), Literal("Hatti_")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test position") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=t a=position('world' IN 'helloworld')", false),
      context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction(
        "position",
        seq(Literal("world"), Literal("helloworld")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test replace") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source=t a=replace('hello', 'l', 'x')", false),
      context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction(
        "replace",
        seq(Literal("hello"), Literal("l"), Literal("x")),
        isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }
}
