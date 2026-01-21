/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanFiltersTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test simple search with only one table with one field literal filtered ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t a = 1 "), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table with two field with 'and' filtered ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = 1 AND b != 2"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterAExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterBExpr = Not(EqualTo(UnresolvedAttribute("b"), Literal(2)))
    val filterPlan = Filter(And(filterAExpr, filterBExpr), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table with two field with 'or' filtered ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = 1 OR b != 2"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterAExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterBExpr = Not(EqualTo(UnresolvedAttribute("b"), Literal(2)))
    val filterPlan = Filter(Or(filterAExpr, filterBExpr), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test simple search with only one table with two field with 'not' filtered ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t not a = 1 or b != 2 "), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterAExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal(1)))
    val filterBExpr = Not(EqualTo(UnresolvedAttribute("b"), Literal(2)))
    val filterPlan = Filter(Or(filterAExpr, filterBExpr), table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field literal int equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field literal string equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, """source=t a = 'hi'  | fields a"""), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal("hi"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field literal string none equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, """source=t a != 'bye'  | fields a"""), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal("bye")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field greater than  filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a > 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = GreaterThan(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field greater than equal  filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a >= 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = GreaterThanOrEqual(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field lower than filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a < 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = LessThan(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field lower than equal filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a <= 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field not equal filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a != 1  | fields a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal(1)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test simple search with only one table with one field not equal filtered and one field projected and sorted") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a != 1  | fields a | sort a"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal(1)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("a"), Ascending)),
        global = true,
        Project(projectList, filterPlan))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test order of evaluation of predicate expression") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "source=employees | where department = 'HR' OR job_title = 'Manager' AND salary > 50000"),
      context)

    val table = UnresolvedRelation(Seq("employees"))
    val filter =
      Filter(
        Or(
          EqualTo(UnresolvedAttribute("department"), Literal("HR")),
          And(
            EqualTo(UnresolvedAttribute("job_title"), Literal("Manager")),
            GreaterThan(UnresolvedAttribute("salary"), Literal(50000)))),
        table)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test IN expr in filter") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t | where a in ('Hello', 'World')"), context)

    val in = In(UnresolvedAttribute("a"), Seq(Literal("Hello"), Literal("World")))
    val filter = Filter(in, UnresolvedRelation(Seq("t")))
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
