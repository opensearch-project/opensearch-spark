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
import org.apache.spark.sql.catalyst.expressions.{EqualTo, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Filter, Join, JoinHint, Project}

class PPLLogicalPlanJoinTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  /** Test table and index name */
  private val testTable1 = "spark_catalog.default.flint_ppl_test1"
  private val testTable2 = "spark_catalog.default.flint_ppl_test2"

  test("test simple join") {
    val context = new CatalystPlanContext
//    val logicalPlan =
//      planTransformer.visit(
//        plan(
//          pplParser,
//          s"""
//           | source = $testTable1 | JOIN $testTable2 ON $testTable1.id = $testTable2.id
//           | """.stripMargin,
//          isExplain = false),
//        context)

    val stat = plan(
      pplParser,
      s"source = $testTable1 JOIN $testTable2 ON $testTable1.id = $testTable2.id",
      isExplain = false)
    val logicalPlan = planTransformer.visit(stat, context)
    val table1 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test1"))
    val table2 = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test2"))

    val joinCondition =
      EqualTo(UnresolvedAttribute(s"$testTable1.id"), UnresolvedAttribute(s"$testTable2.id"))
    val joinPlan = Join(table1, table2, Inner, Some(joinCondition), JoinHint.NONE)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), joinPlan)
    comparePlans(expectedPlan, logicalPlan, checkAnalysis = false)
  }

}
