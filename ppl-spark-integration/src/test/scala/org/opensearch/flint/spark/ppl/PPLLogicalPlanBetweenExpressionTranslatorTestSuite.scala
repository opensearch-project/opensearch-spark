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
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanBetweenExpressionTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test between expression") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = {
      planTransformer.visit(
        plan(
          pplParser,
          "source = table | where datetime_field between '2024-09-10' and  '2024-09-15'"),
        context)
    }
    // SQL: SELECT * FROM table WHERE datetime_field BETWEEN '2024-09-10' AND '2024-09-15'
    val star = Seq(UnresolvedStar(None))

    val datetime_field = UnresolvedAttribute("datetime_field")
    val tableRelation = UnresolvedRelation(Seq("table"))

    val lowerBound = Literal("2024-09-10")
    val upperBound = Literal("2024-09-15")
    val betweenCondition = And(
      GreaterThanOrEqual(datetime_field, lowerBound),
      LessThanOrEqual(datetime_field, upperBound))

    val filterPlan = Filter(betweenCondition, tableRelation)
    val expectedPlan = Project(star, filterPlan)

    comparePlans(expectedPlan, logPlan, false)
  }

}
