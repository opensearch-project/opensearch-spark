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
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 1 | fields c", false),
        context)
    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "b")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(
      seq(UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions included in fields expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, c = 1 | fields a, b, c", false),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "c")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions without fields command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | eval a = 1, b = 1", false), context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "b")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan =
      Project(seq(UnresolvedStar(None)), Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test eval expressions with sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 1 | sort - a | fields b", false),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "b")(exprId = ExprId(1), qualifier = Seq.empty))
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
        plan(pplParser, "source=t | eval a = 1, a = a | sort - a | fields b", false),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(UnresolvedAttribute("a"), "a")(exprId = ExprId(1), qualifier = Seq.empty))
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
        plan(
          pplParser,
          "source=t | eval a = 1, b = 'hello' | eval b = a | sort - b | fields b",
          false),
        context)

    val evalProjectList1: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal("hello"), "b")(exprId = ExprId(1), qualifier = Seq.empty))
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

  // Todo function should work when the PR #448 merged.
  ignore("test complex eval expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = avg(b) | fields a", false),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("avg", seq(UnresolvedAttribute("b")), isDistinct = false), "a")(
        exprId = ExprId(0),
        qualifier = Seq.empty))
    val expectedPlan = Project(
      seq(UnresolvedAttribute("a")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // Todo fields-excluded command not supported
  ignore("test eval expressions with fields-excluded command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 1 | fields - b", false),
        context)

    val projectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "b")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("t")))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // Todo fields-included command not supported
  ignore("test eval expressions with fields-included command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = 1, b = 1 | fields + b", false),
        context)

    val projectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(Literal(1), "a")(exprId = ExprId(0), qualifier = Seq.empty),
      Alias(Literal(1), "b")(exprId = ExprId(1), qualifier = Seq.empty))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("t")))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
