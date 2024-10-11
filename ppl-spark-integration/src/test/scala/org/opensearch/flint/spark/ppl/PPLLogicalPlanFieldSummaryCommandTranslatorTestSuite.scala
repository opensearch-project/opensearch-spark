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
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Project}

class PPLLogicalPlanFieldSummaryCommandTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  ignore("test fieldsummary with `includefields=status_code,user_id,response_time`") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | fieldsummary includefields= status_code, user_id, response_time topvalues=5 nulls=true"),
        context)

    val table = UnresolvedRelation(Seq("t"))

    val renameProjectList: Seq[NamedExpression] =
      Seq(
        UnresolvedStar(None),
        Alias(
          UnresolvedFunction(
            "coalesce",
            Seq(UnresolvedAttribute("column_name"), Literal("null replacement value")),
            isDistinct = false),
          "column_name")())
    val renameProject = Project(renameProjectList, table)

    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("column_name")), renameProject)

    val expectedPlan = Project(seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
