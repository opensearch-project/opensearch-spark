/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, EqualTo, GreaterThanOrEqual, InSubquery, LessThan, ListQuery, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, JoinHint, LogicalPlan, Project, Sample, Sort, SubqueryAlias}

class PPLLogicalPlanInSubqueryTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test where a in (select b from c)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a in [
             |     source = spark_catalog.default.inner | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(Project(Seq(UnresolvedAttribute("b")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where a in (select b from c) with only outer tablesample(50 percent)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer tablesample(50 percent)
             | | where a in [
             |     source = spark_catalog.default.inner | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(Project(Seq(UnresolvedAttribute("b")), inner))),
        Sample(0, 0.5, withReplacement = false, 0, outer))
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where a in (select b from c) with only inner tablesample(50 percent)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a in [
             |     source = spark_catalog.default.inner tablesample(50 percent) | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(
            Project(
              Seq(UnresolvedAttribute("b")),
              Sample(0, 0.5, withReplacement = false, 0, inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test where a in (select b from c) with both inner & outer tables tablesample(50 percent)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer tablesample(50 percent)
             | | where a in [
             |     source = spark_catalog.default.inner tablesample(50 percent) | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(
            Project(
              Seq(UnresolvedAttribute("b")),
              Sample(0, 0.5, withReplacement = false, 0, inner)))),
        Sample(0, 0.5, withReplacement = false, 0, outer))
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where (a) in (select b from c)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a in [
             |     source = spark_catalog.default.inner | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(Project(Seq(UnresolvedAttribute("b")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where (a, b, c) in (select d, e, f from inner)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where (a, b, c) in [
             |     source = spark_catalog.default.inner | fields d, e, f
             |   ]
             | | sort  - a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
          ListQuery(
            Project(
              Seq(UnresolvedAttribute("d"), UnresolvedAttribute("e"), UnresolvedAttribute("f")),
              inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where a not in (select b from c)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a not in [
             |     source = spark_catalog.default.inner | fields b
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        Not(
          InSubquery(
            Seq(UnresolvedAttribute("a")),
            ListQuery(Project(Seq(UnresolvedAttribute("b")), inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where (a, b, c) not in (select d, e, f from inner)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where (a, b, c) not in [
             |     source = spark_catalog.default.inner | fields d, e, f
             |   ]
             | | sort  - a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val inSubquery =
      Filter(
        Not(
          InSubquery(
            Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b"), UnresolvedAttribute("c")),
            ListQuery(
              Project(
                Seq(UnresolvedAttribute("d"), UnresolvedAttribute("e"), UnresolvedAttribute("f")),
                inner)))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubquery)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test nested subquery") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a in [
             |     source = spark_catalog.default.inner1
             |     | where b in [
             |         source = spark_catalog.default.inner2 | fields c
             |       ]
             |     | fields b
             |   ]
             | | sort  - a
             | | fields a, d
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner1 = UnresolvedRelation(Seq("spark_catalog", "default", "inner1"))
    val inner2 = UnresolvedRelation(Seq("spark_catalog", "default", "inner2"))
    val inSubqueryForB =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("b")),
          ListQuery(Project(Seq(UnresolvedAttribute("c")), inner2))),
        inner1)
    val inSubqueryForA =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("a")),
          ListQuery(Project(Seq(UnresolvedAttribute("b")), inSubqueryForB))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, inSubqueryForA)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("d")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  // TODO throw exception with syntax check, now it throw AnalysisException in Spark
  ignore("The number of columns not match output of subquery") {
    val context = new CatalystPlanContext
    val ex = intercept[SyntaxCheckException] {
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where a in [
             |     source = spark_catalog.default.inner | fields b, d
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    }
    assert(ex.getMessage === "The number of columns not match output of subquery")
  }

  test("test tpch q4: in-subquery with aggregation") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = orders
             | | where o_orderdate >= "1993-07-01" AND o_orderdate < "1993-10-01" AND o_orderkey IN
             |   [ source = lineitem
             |     | where l_commitdate < l_receiptdate
             |     | fields l_orderkey
             |   ]
             | | stats count(1) as order_count by o_orderpriority
             | | sort o_orderpriority
             | | fields o_orderpriority, order_count
             | """.stripMargin),
        context)

    val outer = UnresolvedRelation(Seq("orders"))
    val inner = UnresolvedRelation(Seq("lineitem"))
    val inSubquery =
      Filter(
        And(
          And(
            GreaterThanOrEqual(UnresolvedAttribute("o_orderdate"), Literal("1993-07-01")),
            LessThan(UnresolvedAttribute("o_orderdate"), Literal("1993-10-01"))),
          InSubquery(
            Seq(UnresolvedAttribute("o_orderkey")),
            ListQuery(
              Project(
                Seq(UnresolvedAttribute("l_orderkey")),
                Filter(
                  LessThan(
                    UnresolvedAttribute("l_commitdate"),
                    UnresolvedAttribute("l_receiptdate")),
                  inner))))),
        outer)
    val o_orderpriorityAlias = Alias(UnresolvedAttribute("o_orderpriority"), "o_orderpriority")()
    val groupByAttributes = Seq(o_orderpriorityAlias)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(Seq("COUNT"), Seq(Literal(1)), isDistinct = false),
        "order_count")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, o_orderpriorityAlias), inSubquery)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("o_orderpriority"), Ascending)),
        global = true,
        aggregatePlan)
    val expectedPlan = Project(
      Seq(UnresolvedAttribute("o_orderpriority"), UnresolvedAttribute("order_count")),
      sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test tpch q20 (partial): nested in-subquery") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = supplier
             | | where s_suppkey IN [
             |     source = partsupp
             |     | where ps_partkey IN [
             |         source = part
             |         | where like(p_name, "forest%")
             |         | fields p_partkey
             |       ]
             |     | fields ps_suppkey
             |   ]
             | | inner join left=l right=r on s_nationkey = n_nationkey and n_name = 'CANADA'
             |   nation
             | | sort s_name
             | """.stripMargin),
        context)

    val outer = UnresolvedRelation(Seq("supplier"))
    val inner = UnresolvedRelation(Seq("partsupp"))
    val nestedInner = UnresolvedRelation(Seq("part"))
    val right = UnresolvedRelation(Seq("nation"))
    val inSubquery =
      Filter(
        InSubquery(
          Seq(UnresolvedAttribute("s_suppkey")),
          ListQuery(
            Project(
              Seq(UnresolvedAttribute("ps_suppkey")),
              Filter(
                InSubquery(
                  Seq(UnresolvedAttribute("ps_partkey")),
                  ListQuery(Project(
                    Seq(UnresolvedAttribute("p_partkey")),
                    Filter(
                      UnresolvedFunction(
                        "like",
                        Seq(UnresolvedAttribute("p_name"), Literal("forest%")),
                        isDistinct = false),
                      nestedInner)))),
                inner)))),
        outer)
    val leftPlan = SubqueryAlias("l", inSubquery)
    val rightPlan = SubqueryAlias("r", right)
    val joinCondition =
      And(
        EqualTo(UnresolvedAttribute("s_nationkey"), UnresolvedAttribute("n_nationkey")),
        EqualTo(UnresolvedAttribute("n_name"), Literal("CANADA")))
    val joinPlan = Join(leftPlan, rightPlan, Inner, Some(joinCondition), JoinHint.NONE)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("s_name"), Ascending)), global = true, joinPlan)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
}
