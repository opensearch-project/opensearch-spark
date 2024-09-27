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
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, ExprId, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Project, Sort}

class PPLLogicalPlanEvalTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test eval expressions not included in fields expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = 1, b = 1 | fields c"), context)
    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions included in fields expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, c = 1 | fields a, b, c"),
        context)

    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "c")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions without fields command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = 1, b = 1"), context)

    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val expectedPlan =
      Project(seq(UnresolvedStar(None)), Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions with sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 1 | sort - a | fields b"),
        context)

    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val evalProject = Project(evalProjectList, UnresolvedRelation(Seq("t")))
    val sortOrder = SortOrder(UnresolvedAttribute("a"), Descending, Seq.empty)
    val sort = Sort(seq(sortOrder), global = true, evalProject)
    val expectedPlan = Project(seq(UnresolvedAttribute("b")), sort)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions with multiple recursive sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, a = a | sort - a | fields b"),
        context)

    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(UnresolvedAttribute("a"), "a")())
    val evalProject = Project(evalProjectList, UnresolvedRelation(Seq("t")))
    val sortOrder = SortOrder(UnresolvedAttribute("a"), Descending, Seq.empty)
    val sort = Sort(seq(sortOrder), global = true, evalProject)
    val expectedPlan = Project(seq(UnresolvedAttribute("b")), sort)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test multiple eval expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 'hello' | eval b = a | sort - b | fields b"),
        context)

    val evalProjectList1: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal("hello"), "b")())
    val evalProjectList2: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedAttribute("a"), "b")(exprId = ExprId(2), qualifier = Seq.empty))
    val evalProject1 = Project(evalProjectList1, UnresolvedRelation(Seq("t")))
    val evalProject2 = Project(evalProjectList2, evalProject1)
    val sortOrder = SortOrder(UnresolvedAttribute("b"), Descending, Seq.empty)
    val sort = Sort(seq(sortOrder), global = true, evalProject2)
    val expectedPlan = Project(seq(UnresolvedAttribute("b")), sort)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test complex eval expressions - date function") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = TIMESTAMP('2020-09-16 17:30:00') | fields a"),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction("timestamp", seq(Literal("2020-09-16 17:30:00")), isDistinct = false),
        "a")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test complex eval expressions - math function") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = RAND() | fields a"), context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("rand", Seq.empty, isDistinct = false), "a")(
        exprId = ExprId(0),
        qualifier = Seq.empty))
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test complex eval expressions - compound function") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = if(like(b, '%Hello%'), 'World', 'Hi') | fields a"),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(
        UnresolvedFunction(
          "if",
          seq(
            UnresolvedFunction(
              "like",
              seq(UnresolvedAttribute("b"), Literal("%Hello%")),
              isDistinct = false),
            Literal("World"),
            Literal("Hi")),
          isDistinct = false),
        "a")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // Todo fields-excluded command not supported
  ignore("test eval expressions with fields-excluded command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = 1, b = 2 | fields - b"), context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(2), "b")())
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("t")))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // Todo fields-included command not supported
  ignore("test eval expressions with fields-included command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = 1, b = 2 | fields + b"), context)

    val projectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(2), "b")())
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("t")))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
