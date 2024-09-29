/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Descending, InSubquery, ListQuery, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sort}

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
    val ex = intercept[UnsupportedOperationException] {
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
}
