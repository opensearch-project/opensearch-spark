/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}

class PPLLogicalPlanTimeFunctionsTranslatorTestSuite
    extends SparkFunSuite
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test from_unixtime") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = from_unixtime(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("from_unixtime", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }

  test("test unix_timestamp") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, "source=t a = unix_timestamp(b)", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(
      UnresolvedAttribute("a"),
      UnresolvedFunction("unix_timestamp", seq(UnresolvedAttribute("b")), isDistinct = false))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, logPlan)
  }
}
