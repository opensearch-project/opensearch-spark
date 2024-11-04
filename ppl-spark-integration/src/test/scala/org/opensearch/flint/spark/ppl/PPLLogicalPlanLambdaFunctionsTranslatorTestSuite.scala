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
import org.apache.spark.sql.catalyst.expressions.{Alias, GreaterThan, LambdaFunction, Literal, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project

class PPLLogicalPlanLambdaFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test forall()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = forall(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(
      GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)),
      Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB =
      Alias(UnresolvedFunction("forall", Seq(UnresolvedAttribute("a"), lambda), false), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test exits()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = exists(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(
      GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)),
      Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB =
      Alias(UnresolvedFunction("exists", Seq(UnresolvedAttribute("a"), lambda), false), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test filter()") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = filter(a, x -> x > 0)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(
      GreaterThan(UnresolvedNamedLambdaVariable(seq("x")), Literal(0)),
      Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB =
      Alias(UnresolvedFunction("filter", Seq(UnresolvedAttribute("a"), lambda), false), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test transform()") {
    val context = new CatalystPlanContext
    // test single argument of lambda
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = transform(a, x -> x + 1)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(
      UnresolvedFunction("+", Seq(UnresolvedNamedLambdaVariable(seq("x")), Literal(1)), false),
      Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB =
      Alias(UnresolvedFunction("transform", Seq(UnresolvedAttribute("a"), lambda), false), "b")()
    val evalProject = Project(Seq(UnresolvedStar(None), aliasA, aliasB), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, evalProject)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test transform() - test double arguments of lambda") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = transform(a, (x, y) -> x + y)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val lambda = LambdaFunction(
      UnresolvedFunction(
        "+",
        Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))),
        false),
      Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))))
    val aliasB =
      Alias(UnresolvedFunction("transform", Seq(UnresolvedAttribute("a"), lambda), false), "b")()
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      Project(Seq(UnresolvedStar(None), aliasA, aliasB), table))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test reduce() - without finish lambda") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = reduce(a, 0, (x, y) -> x + y)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val mergeLambda = LambdaFunction(
      UnresolvedFunction(
        "+",
        Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))),
        false),
      Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))))
    val aliasB =
      Alias(
        UnresolvedFunction(
          "reduce",
          Seq(UnresolvedAttribute("a"), Literal(0), mergeLambda),
          false),
        "b")()
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      Project(Seq(UnresolvedStar(None), aliasA, aliasB), table))
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test reduce() - with finish lambda") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          """source=t | eval a = json_array(1, 2, 3), b = transform(a, 0, (x, y) -> x + y, x -> x * 10)""".stripMargin),
        context)
    val table = UnresolvedRelation(Seq("t"))
    val jsonFunc =
      UnresolvedFunction("array", Seq(Literal(1), Literal(2), Literal(3)), isDistinct = false)
    val aliasA = Alias(jsonFunc, "a")()
    val mergeLambda = LambdaFunction(
      UnresolvedFunction(
        "+",
        Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))),
        false),
      Seq(UnresolvedNamedLambdaVariable(seq("x")), UnresolvedNamedLambdaVariable(seq("y"))))
    val finishLambda = LambdaFunction(
      UnresolvedFunction("*", Seq(UnresolvedNamedLambdaVariable(seq("x")), Literal(10)), false),
      Seq(UnresolvedNamedLambdaVariable(seq("x"))))
    val aliasB =
      Alias(
        UnresolvedFunction(
          "transform",
          Seq(UnresolvedAttribute("a"), Literal(0), mergeLambda, finishLambda),
          false),
        "b")()
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      Project(Seq(UnresolvedStar(None), aliasA, aliasB), table))
    comparePlans(expectedPlan, logPlan, false)
  }
}
