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
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, Project}

class PPLLogicalPlanFieldSummaryCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test fieldsummary with single field includefields(status_code) & nulls=true") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | fieldsummary includefields= status_code nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

    // Aggregate with functions applied to status_code
    val aggregateExpressions: Seq[NamedExpression] = Seq(
      Alias(Literal("status_code"), "Field")(),
      Alias(UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "COUNT(status_code)")(),
      Alias(UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = true), "COUNT_DISTINCT(status_code)")(),
      Alias(UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "MIN(status_code)")(),
      Alias(UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "MAX(status_code)")(),
      Alias(UnresolvedFunction("AVG", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "AVG(status_code)")(),
      Alias(UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "TYPEOF(status_code)")()
    )

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false), "TYPEOF(status_code)")()),
      aggregateExpressions,
      table
    )
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }
}
