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
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanCryptographicFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test md5") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t  a = md5(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("md5", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test sha1") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t  a = sha1(b)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("sha1", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test sha2") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(plan(pplParser, "source=t  a = sha2(b,256)"), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("sha2", seq(UnresolvedAttribute("b"), Literal(256)), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    comparePlans(expectedPlan, logPlan, false)
  }
}
