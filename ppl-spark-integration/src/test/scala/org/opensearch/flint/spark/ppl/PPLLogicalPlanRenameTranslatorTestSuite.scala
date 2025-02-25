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
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, ExprId, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Project, Sort}

class PPLLogicalPlanRenameTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test renamed fields not included in fields expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | rename a as r_a, b as r_b | fields c"),
        context)
    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("a"), "r_a")(),
        Alias(UnresolvedAttribute("b"), "r_b")())
    val innerProject = Project(renameProjectList, UnresolvedRelation(Seq("t")))
    val planDropColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), innerProject)
    val expectedPlan = Project(seq(UnresolvedAttribute("c")), planDropColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test renamed fields included in fields expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | rename a as r_a, b as r_b | fields r_a, r_b, c"),
        context)

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("a"), "r_a")(),
        Alias(UnresolvedAttribute("b"), "r_b")())
    val innerProject = Project(renameProjectList, UnresolvedRelation(Seq("t")))
    val planDropColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), innerProject)
    val expectedPlan = Project(
      seq(UnresolvedAttribute("r_a"), UnresolvedAttribute("r_b"), UnresolvedAttribute("c")),
      planDropColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test renamed fields without fields command") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | rename a as r_a, b as r_b"), context)

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("a"), "r_a")(),
        Alias(UnresolvedAttribute("b"), "r_b")())
    val innerProject = Project(renameProjectList, UnresolvedRelation(Seq("t")))
    val planDropColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), innerProject)
    val expectedPlan =
      Project(seq(UnresolvedStar(None)), planDropColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test renamed fields with sort") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | rename a as r_a, b as r_b | sort - r_a | fields r_b"),
        context)

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(UnresolvedAttribute("a"), "r_a")(),
        Alias(UnresolvedAttribute("b"), "r_b")())
    val renameProject = Project(renameProjectList, UnresolvedRelation(Seq("t")))
    val planDropColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), renameProject)
    val sortOrder = SortOrder(UnresolvedAttribute("r_a"), Descending, Seq.empty)
    val sort = Sort(seq(sortOrder), global = true, planDropColumn)
    val expectedPlan = Project(seq(UnresolvedAttribute("r_b")), sort)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test rename eval expression output") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = RAND() | rename a as eval_rand | fields eval_rand"),
        context)

    val evalProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("rand", Seq.empty, isDistinct = false), "a")(
        exprId = ExprId(0),
        qualifier = Seq.empty))
    val evalProject = Project(evalProjectList, UnresolvedRelation(Seq("t")))
    val renameProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(UnresolvedAttribute("a"), "eval_rand")())
    val innerProject = Project(renameProjectList, evalProject)
    val planDropColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("a")), innerProject)
    val expectedPlan =
      Project(seq(UnresolvedAttribute("eval_rand")), planDropColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test rename with backticks alias") {
    val expectedPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | rename a as r_a, b as r_b | fields c"),
        new CatalystPlanContext)
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | rename `a` as `r_a`, `b` as `r_b` | fields `c`"),
        new CatalystPlanContext)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
