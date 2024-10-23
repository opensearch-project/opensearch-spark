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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, EqualTo, GreaterThan, Literal, Or, ScalarSubquery, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

/** Assume the table outer contains column a and b, table inner contains column c and d */
class PPLLogicalPlanScalarSubqueryTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test uncorrelated scalar subquery in select") {
    // select (select max(c) as max_c from inner), a from outer
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c = [
             |     source = spark_catalog.default.inner | stats max(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, inner)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), evalProject)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery in expression in select") {
    // select (select max(c) as max_c from inner) + a, b from outer
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c_plus_a = [
             |     source = spark_catalog.default.inner | stats max(c)
             |   ] + a
             | | fields max_c, b
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, inner)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val scalarSubqueryPlusC = UnresolvedFunction(
      Seq("+"),
      Seq(scalarSubqueryExpr, UnresolvedAttribute("a")),
      isDistinct = false)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryPlusC, "max_c_plus_a")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("b")), evalProject)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test uncorrelated scalar subquery in select and where") {
    // select (select max(c) from inner), a from outer where b > (select min(c) from inner)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c = [
             |     source = spark_catalog.default.inner | stats max(c)
             |   ]
             | | where b > [
             |     source = spark_catalog.default.inner | stats min(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val maxAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val minAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MIN"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "min(c)")())
    val maxAggPlan = Aggregate(Seq(), maxAgg, inner)
    val minAggPlan = Aggregate(Seq(), minAgg, inner)
    val maxScalarSubqueryExpr = ScalarSubquery(maxAggPlan)
    val minScalarSubqueryExpr = ScalarSubquery(minAggPlan)

    val evalProjectList = Seq(UnresolvedStar(None), Alias(maxScalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, outer)
    val filter = Filter(GreaterThan(UnresolvedAttribute("b"), minScalarSubqueryExpr), evalProject)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test(
    "test uncorrelated scalar subquery in select and where with outer sample(50 percent)") {
    // select (select max(c) from inner), a from outer where b > (select min(c) from inner)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer sample(50 percent)
             | | eval max_c = [
             |     source = spark_catalog.default.inner | stats max(c)
             |   ]
             | | where b > [
             |     source = spark_catalog.default.inner | stats min(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val maxAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val minAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MIN"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "min(c)")())
    val maxAggPlan = Aggregate(Seq(), maxAgg, inner)
    val minAggPlan = Aggregate(Seq(), minAgg, inner)
    val maxScalarSubqueryExpr = ScalarSubquery(maxAggPlan)
    val minScalarSubqueryExpr = ScalarSubquery(minAggPlan)

    val evalProjectList = Seq(UnresolvedStar(None), Alias(maxScalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, Sample(0, 0.5, withReplacement = false, 0, outer))
    val filter = Filter(GreaterThan(UnresolvedAttribute("b"), minScalarSubqueryExpr), evalProject)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test(
    "test uncorrelated scalar subquery in select and where with inner sample(50 percent) for max_c eval") {
    // select (select max(c) from inner), a from outer where b > (select min(c) from inner)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c = [
             |     source = spark_catalog.default.inner sample(50 percent) | stats max(c)
             |   ]
             | | where b > [
             |     source = spark_catalog.default.inner | stats min(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val maxAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val minAgg = Seq(
      Alias(
        UnresolvedFunction(Seq("MIN"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "min(c)")())
    val maxAggPlan = Aggregate(Seq(), maxAgg, Sample(0, 0.5, withReplacement = false, 0, inner))
    val minAggPlan = Aggregate(Seq(), minAgg, inner)
    val maxScalarSubqueryExpr = ScalarSubquery(maxAggPlan)
    val minScalarSubqueryExpr = ScalarSubquery(minAggPlan)

    val evalProjectList = Seq(UnresolvedStar(None), Alias(maxScalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, outer)
    val filter = Filter(GreaterThan(UnresolvedAttribute("b"), minScalarSubqueryExpr), evalProject)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), filter)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in select") {
    // select (select max(c) from inner where b = d), a from outer
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c = [
             |     source = spark_catalog.default.inner | where b = d | stats max(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val filter = Filter(EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), evalProject)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in select with both tables sample(50 percent)") {
    // select (select max(c) from inner where b = d), a from outer
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer sample(50 percent)
             | | eval max_c = [
             |     source = spark_catalog.default.inner sample(50 percent) | where b = d | stats max(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val filter = Filter(
      EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")),
      Sample(0, 0.5, withReplacement = false, 0, inner))
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, Sample(0, 0.5, withReplacement = false, 0, outer))
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), evalProject)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in select with non-equal") {
    // select (select max(c) from inner where b > d), a from outer
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval max_c = [
             |     source = spark_catalog.default.inner | where b > d | stats max(c)
             |   ]
             | | fields max_c, a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val filter = Filter(GreaterThan(UnresolvedAttribute("b"), UnresolvedAttribute("d")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, filter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val evalProjectList = Seq(UnresolvedStar(None), Alias(scalarSubqueryExpr, "max_c")())
    val evalProject = Project(evalProjectList, outer)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("max_c"), UnresolvedAttribute("a")), evalProject)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test correlated scalar subquery in where") {
    // select * from outer where a = (select max(c) from inner where b = d)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a = [
             |     source = spark_catalog.default.inner | where b = d | stats max(c)
             |   ]
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false),
        "max(c)")())
    val innerFilter = Filter(EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")), inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(EqualTo(UnresolvedAttribute("a"), scalarSubqueryExpr), outer)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), outerFilter)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test disjunctive correlated scalar subquery") {
    // select a from outer where (select count(*) from inner where b = d or d = 1> 0)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where [
             |     source = spark_catalog.default.inner | where b = d OR d = 1 | stats count()
             |   ] > 0
             | | fields a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "count()")())
    val innerFilter =
      Filter(
        Or(
          EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")),
          EqualTo(UnresolvedAttribute("d"), Literal(1))),
        inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(GreaterThan(scalarSubqueryExpr, Literal(0)), outer)
    val expectedPlan = Project(Seq(UnresolvedAttribute("a")), outerFilter)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // TODO a bug when the filter contains parenthetic expressions
  ignore("test disjunctive correlated scalar subquery 2") {
    // select c
    // from   outer
    // where  (select count(*)
    //        from   inner
    //        where (b = d and b = 2) or (b = d and d = 1)) > 0
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where [
             |     source = spark_catalog.default.inner | where b = d AND b = 2 OR b = d AND b = 1 | stats count()
             |   ] > 0
             | | fields c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val aggregateExpressions = Seq(
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
        "count()")())
    val innerFilter =
      Filter(
        Or(
          And(
            EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")),
            EqualTo(UnresolvedAttribute("b"), Literal(2))),
          And(
            EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d")),
            EqualTo(UnresolvedAttribute("b"), Literal(1)))),
        inner)
    val aggregatePlan = Aggregate(Seq(), aggregateExpressions, innerFilter)
    val scalarSubqueryExpr = ScalarSubquery(aggregatePlan)
    val outerFilter = Filter(GreaterThan(scalarSubqueryExpr, Literal(0)), outer)
    val expectedPlan = Project(Seq(UnresolvedAttribute("c")), outerFilter)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test two scalar subqueries in OR") {
    // SELECT * FROM outer
    // WHERE  a = (SELECT max(c)
    //             FROM   inner
    //             ORDER BY c)
    // OR     b = (SELECT min(d)
    //             FROM   inner
    //             WHERE  c = 1
    //             ORDER BY d)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a = [
             |     source = spark_catalog.default.inner | stats max(c) | sort c
             |   ] OR b = [
             |     source = spark_catalog.default.inner | where c = 1 | stats min(d) | sort d
             |   ]
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val maxExpr =
      UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("c")), isDistinct = false)
    val minExpr =
      UnresolvedFunction(Seq("MIN"), Seq(UnresolvedAttribute("d")), isDistinct = false)
    val maxAgg = Seq(Alias(maxExpr, "max(c)")())
    val minAgg = Seq(Alias(minExpr, "min(d)")())
    val maxAggPlan = Aggregate(Seq(), maxAgg, inner)
    val minAggPlan =
      Aggregate(Seq(), minAgg, Filter(EqualTo(UnresolvedAttribute("c"), Literal(1)), inner))
    val subquery1 =
      Sort(Seq(SortOrder(UnresolvedAttribute("c"), Ascending)), global = true, maxAggPlan)
    val maxScalarSubqueryExpr = ScalarSubquery(subquery1)
    val subquery2 =
      Sort(Seq(SortOrder(UnresolvedAttribute("d"), Ascending)), global = true, minAggPlan)
    val minScalarSubqueryExpr = ScalarSubquery(subquery2)
    val filterOr = Filter(
      Or(
        EqualTo(UnresolvedAttribute("a"), maxScalarSubqueryExpr),
        EqualTo(UnresolvedAttribute("b"), minScalarSubqueryExpr)),
      outer)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filterOr)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  /**
   * table outer contains column a and b, table inner1 contains column c and d, table inner2
   * contains column e and f
   */
  test("test nested scalar subquery") {
    // SELECT *
    // FROM   outer
    // WHERE  a = (SELECT   max(c)
    //             FROM     inner1
    //             WHERE c = (SELECT   max(e)
    //                        FROM     inner2
    //                        GROUP BY f
    //                        ORDER BY f
    //                        )
    //             GROUP BY c
    //             ORDER BY c
    //             LIMIT 1)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a = [
             |     source = spark_catalog.default.inner1
             |     | where c = [
             |         source = spark_catalog.default.inner2
             |         | stats max(e) by f
             |         | sort f
             |       ]
             |     | stats max(d) by c
             |     | sort c
             |     | head 1
             |   ]
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner1 = UnresolvedRelation(Seq("spark_catalog", "default", "inner1"))
    val inner2 = UnresolvedRelation(Seq("spark_catalog", "default", "inner2"))
    val maxExprE =
      UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("e")), isDistinct = false)
    val aggMaxE = Seq(Alias(maxExprE, "max(e)")(), Alias(UnresolvedAttribute("f"), "f")())
    val aggregateInner = Aggregate(Seq(Alias(UnresolvedAttribute("f"), "f")()), aggMaxE, inner2)
    val subqueryInner =
      Sort(Seq(SortOrder(UnresolvedAttribute("f"), Ascending)), global = true, aggregateInner)
    val filterInner =
      Filter(EqualTo(UnresolvedAttribute("c"), ScalarSubquery(subqueryInner)), inner1)
    val maxExprD =
      UnresolvedFunction(Seq("MAX"), Seq(UnresolvedAttribute("d")), isDistinct = false)
    val aggMaxD = Seq(Alias(maxExprD, "max(d)")(), Alias(UnresolvedAttribute("c"), "c")())
    val aggregateOuter =
      Aggregate(Seq(Alias(UnresolvedAttribute("c"), "c")()), aggMaxD, filterInner)
    val sort =
      Sort(Seq(SortOrder(UnresolvedAttribute("c"), Ascending)), global = true, aggregateOuter)
    val subqueryOuter = GlobalLimit(Literal(1), LocalLimit(Literal(1), sort))
    val filterOuter =
      Filter(EqualTo(UnresolvedAttribute("a"), ScalarSubquery(subqueryOuter)), outer)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), filterOuter)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  // TODO eval command with stats function is unsupported
  ignore("test nested scalar subquery 2") {
    // SELECT *
    // FROM   outer
    // WHERE  a = (SELECT   max(c)
    //             FROM     inner1
    //             WHERE c = (SELECT   max(e)
    //                        FROM     inner2
    //                        GROUP BY f
    //                        ORDER BY max(e)
    //                        )
    //             GROUP BY c
    //             ORDER BY max(e)
    //             LIMIT 1)
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a = [
             |     source = spark_catalog.default.inner1
             |     | where c = [
             |         source = spark_catalog.default.inner2
             |         | eval max_e = max(e)
             |         | stats max(e) by f
             |         | sort max_e
             |       ]
             |     | eval max_c = max(c)
             |     | stats max(e) by c
             |     | sort max_c
             |     | head 1
             |   ]
             | """.stripMargin),
        context)
  }

  // TODO currently statsBy expression is unsupported.
  ignore("test correlated scalar subquery in group by") {
    // select b, (select count(a) from inner where b = d) count_a from outer group by 1, 2
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | eval count_a = [
             |     source = spark_catalog.default.inner | where b = d | stats count(a)
             |   ]
             | | stats by b, count_a
             | """.stripMargin),
        context)
  }
}
