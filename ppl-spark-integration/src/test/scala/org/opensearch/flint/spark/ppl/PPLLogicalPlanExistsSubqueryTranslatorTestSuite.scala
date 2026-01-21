/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Descending, EqualTo, Exists, GreaterThanOrEqual, LessThan, Literal, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanExistsSubqueryTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  // Assume outer table contains fields [a, b]
  // and inner table contains fields [c, d]
  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test where exists (select * from inner where a = c)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where exists [
             |     source = spark_catalog.default.inner | where a = c
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val subquery =
      Filter(
        Exists(Filter(EqualTo(UnresolvedAttribute("a"), UnresolvedAttribute("c")), inner)),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, subquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where exists (select * from inner where a = c and b = d)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where exists [
             |     source = spark_catalog.default.inner | where a = c AND b = d
             |   ]
             | | sort  - a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val existsSubquery =
      Filter(
        Exists(
          Filter(
            And(
              EqualTo(UnresolvedAttribute("a"), UnresolvedAttribute("c")),
              EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d"))),
            inner)),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, existsSubquery)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where not exists (select * from inner where a = c)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where not exists [
             |     source = spark_catalog.default.inner | where a = c
             |   ]
             | | sort  - a
             | | fields a, c
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val subquery =
      Filter(
        Not(Exists(Filter(EqualTo(UnresolvedAttribute("a"), UnresolvedAttribute("c")), inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, subquery)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("c")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test where not exists (select * from inner where a = c and b = d)") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where not exists [
             |     source = spark_catalog.default.inner | where a = c AND b = d
             |   ]
             | | sort  - a
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner = UnresolvedRelation(Seq("spark_catalog", "default", "inner"))
    val existsSubquery =
      Filter(
        Not(
          Exists(
            Filter(
              And(
                EqualTo(UnresolvedAttribute("a"), UnresolvedAttribute("c")),
                EqualTo(UnresolvedAttribute("b"), UnresolvedAttribute("d"))),
              inner))),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, existsSubquery)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  // Assume outer table contains fields [a, b]
  // and inner1 table contains fields [c, d]
  // and inner2 table contains fields [e, f]
  test("test nested exists subquery") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = spark_catalog.default.outer
             | | where exists [
             |     source = spark_catalog.default.inner1
             |     | where exists [
             |         source = spark_catalog.default.inner2
             |         | where c = e
             |       ]
             |     | where a = c
             |   ]
             | | sort  - a
             | | fields a, b
             | """.stripMargin),
        context)
    val outer = UnresolvedRelation(Seq("spark_catalog", "default", "outer"))
    val inner1 = UnresolvedRelation(Seq("spark_catalog", "default", "inner1"))
    val inner2 = UnresolvedRelation(Seq("spark_catalog", "default", "inner2"))
    val subqueryOuter =
      Filter(
        Exists(Filter(EqualTo(UnresolvedAttribute("c"), UnresolvedAttribute("e")), inner2)),
        inner1)
    val subqueryInner =
      Filter(
        Exists(
          Filter(EqualTo(UnresolvedAttribute("a"), UnresolvedAttribute("c")), subqueryOuter)),
        outer)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("a"), Descending)), global = true, subqueryInner)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("a"), UnresolvedAttribute("b")), sortedPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

  test("test tpch q4: exists subquery with aggregation") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = orders
             | | where o_orderdate >= "1993-07-01" AND o_orderdate < "1993-10-01"
             |     AND exists [
             |       source = lineitem
             |       | where l_orderkey = o_orderkey
             |           AND l_commitdate < l_receiptdate
             |     ]
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
          Exists(
            Filter(
              And(
                EqualTo(UnresolvedAttribute("l_orderkey"), UnresolvedAttribute("o_orderkey")),
                LessThan(
                  UnresolvedAttribute("l_commitdate"),
                  UnresolvedAttribute("l_receiptdate"))),
              inner))),
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

  // We can support q21 when the table alias is supported
  ignore("test tpch q21 (partial): multiple exists subquery") {
    // select
    //    s_name,
    //    count(*) as numwait
    // from
    //    supplier,
    //    lineitem l1,
    // where
    //    s_suppkey = l1.l_suppkey
    //    and l1.l_receiptdate > l1.l_commitdate
    //    and exists (
    //        select
    //            *
    //        from
    //            lineitem l2
    //        where
    //            l2.l_orderkey = l1.l_orderkey
    //            and l2.l_suppkey <> l1.l_suppkey
    //    )
    //    and not exists (
    //        select
    //            *
    //        from
    //            lineitem l3
    //        where
    //            l3.l_orderkey = l1.l_orderkey
    //            and l3.l_suppkey <> l1.l_suppkey
    //            and l3.l_receiptdate > l3.l_commitdate
    //    )
    // group by
    //    s_name
    // order by
    //    numwait desc,
    //    s_name
    // limit 100
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          s"""
             | source = supplier
             | | join left=s right=l1 on s_suppkey = l1.l_suppkey
             |   lineitem as l1
             | | where l1.l_receiptdate > l1.l_commitdate
             | | where exists [
             |     source = lineitem as l2
             |     | where l2.l_orderkey = l1.l_orderkey and
             |         l2.l_suppkey <> l1.l_suppkey
             |   ]
             | | where not exists [
             |     source = lineitem as l3
             |     | where l3.l_orderkey = l1.l_orderkey and
             |        l3.l_suppkey <> l1.l_suppkey and
             |        l3.l_receiptdate > l3.l_commitdate
             |   ]
             | | stats count(1) as numwait by s_name
             | | sort - numwait, s_name
             | | fields s_name, numwait
             | | limit 100
             | """.stripMargin),
        context)
  }
}
