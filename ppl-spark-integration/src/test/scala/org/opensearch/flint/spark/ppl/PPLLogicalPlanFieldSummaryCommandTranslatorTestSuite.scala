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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DataFrameDropColumns, Project, Union}

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
        plan(pplParser, "source = t | fieldsummary includefields= status_code nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

    // Aggregate with functions applied to status_code
    val aggregateExpressions: Seq[NamedExpression] = Seq(
      Alias(Literal("status_code"), "Field")(),
      Alias(
        UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "COUNT(status_code)")(),
      Alias(
        UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = true),
        "COUNT_DISTINCT(status_code)")(),
      Alias(
        UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MIN(status_code)")(),
      Alias(
        UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "MAX(status_code)")(),
      Alias(
        UnresolvedFunction("AVG", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "AVG(status_code)")(),
      Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF(status_code)")())

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      groupingExpressions = Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF(status_code)")()),
      aggregateExpressions,
      table)
    val expectedPlan = Project(seq(UnresolvedStar(None)), aggregatePlan)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }

  test(
    "test fieldsummary with single field includefields(id, status_code, request_path) & nulls=true") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | fieldsummary includefields= id, status_code, request_path nulls=true"),
        context)

    // Define the table
    val table = UnresolvedRelation(Seq("t"))

    // Aggregate with functions applied to status_code
    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregateIdPlan = Aggregate(
      Seq(
        Alias(
          UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "TYPEOF(id)")()),
      Seq(
        Alias(Literal("id"), "Field")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "COUNT(id)")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("id")), isDistinct = true),
          "COUNT_DISTINCT(id)")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MIN(id)")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "MAX(id)")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "AVG(id)")(),
        Alias(
          UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("id")), isDistinct = false),
          "TYPEOF(id)")()),
      table)
    val idProj = Project(seq(UnresolvedStar(None)), aggregateIdPlan)

    // Aggregate with functions applied to status_code
    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregateStatusCodePlan = Aggregate(
      Seq(Alias(
        UnresolvedFunction("TYPEOF", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
        "TYPEOF(status_code)")()),
      Seq(
        Alias(Literal("status_code"), "Field")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "COUNT(status_code)")(),
        Alias(
          UnresolvedFunction("COUNT", Seq(UnresolvedAttribute("status_code")), isDistinct = true),
          "COUNT_DISTINCT(status_code)")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MIN(status_code)")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "MAX(status_code)")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("status_code")), isDistinct = false),
          "AVG(status_code)")(),
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("status_code")),
            isDistinct = false),
          "TYPEOF(status_code)")()),
      table)
    val statusCodeProj = Project(seq(UnresolvedStar(None)), aggregateStatusCodePlan)

    // Define the aggregate plan with alias for TYPEOF in the aggregation
    val aggregatePlan = Aggregate(
      Seq(
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "TYPEOF(request_path)")()),
      Seq(
        Alias(Literal("request_path"), "Field")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "COUNT(request_path)")(),
        Alias(
          UnresolvedFunction(
            "COUNT",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = true),
          "COUNT_DISTINCT(request_path)")(),
        Alias(
          UnresolvedFunction("MIN", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MIN(request_path)")(),
        Alias(
          UnresolvedFunction("MAX", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "MAX(request_path)")(),
        Alias(
          UnresolvedFunction("AVG", Seq(UnresolvedAttribute("request_path")), isDistinct = false),
          "AVG(request_path)")(),
        Alias(
          UnresolvedFunction(
            "TYPEOF",
            Seq(UnresolvedAttribute("request_path")),
            isDistinct = false),
          "TYPEOF(request_path)")()),
      table)
    val requestPathProj = Project(seq(UnresolvedStar(None)), aggregatePlan)

    val expectedPlan = Union(seq(idProj, statusCodeProj, requestPathProj), true, true)
    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }
}
