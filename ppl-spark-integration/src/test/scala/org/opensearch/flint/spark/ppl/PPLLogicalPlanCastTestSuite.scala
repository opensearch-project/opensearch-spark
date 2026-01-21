/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.flint.spark.ppl.legacy.common.antlr.SyntaxCheckException
import org.opensearch.flint.spark.ppl.legacy.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.flint.spark.ppl.legacy.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{IntegerType, StringType}

class PPLLogicalPlanCastTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test cast with case sensitive") {
    val table = UnresolvedRelation(Seq("t"))
    val expectedPlan = Project(
      seq(UnresolvedStar(None)),
      Project(
        seq(UnresolvedStar(None), Alias(Cast(UnresolvedAttribute("a"), StringType), "a")()),
        table))

    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(plan(pplParser, """source=t | eval a = cast(a as STRING)"""), context)
    comparePlans(expectedPlan, logPlan, false)

    // test case insensitive
    val context2 = new CatalystPlanContext
    val logPlan2 =
      planTransformer.visit(
        plan(pplParser, """source=t | eval a = cast(a as string)"""),
        context2)
    comparePlans(expectedPlan, logPlan2, false)
  }

  test("test cast literal") {
    val table = UnresolvedRelation(Seq("t"))
    val expectedPlan = Project(
      seq(UnresolvedStar(None)),
      Project(
        seq(
          UnresolvedStar(None),
          Alias(Cast(Cast(Literal("a"), IntegerType), StringType), "a")()),
        table))

    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t | eval a = cast(cast("a" as INTEGER) as STRING)"""),
        context)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test chained cast") {
    val table = UnresolvedRelation(Seq("t"))
    val expectedPlan = Project(
      seq(UnresolvedStar(None)),
      Project(
        seq(
          UnresolvedStar(None),
          Alias(Cast(Cast(UnresolvedAttribute("a"), IntegerType), StringType), "a")()),
        table))

    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, """source=t | eval a = cast(cast(a as INTEGER) as STRING)"""),
        context)
    comparePlans(expectedPlan, logPlan, false)
  }

  test("test cast with unsupported dataType") {
    // Unsupported data type for opensearch parser
    val context = new CatalystPlanContext
    val exception = intercept[SyntaxCheckException] {
      planTransformer.visit(
        plan(pplParser, """source=t | eval a = cast(a as UNSUPPORTED_DATATYPE)"""),
        context)
    }
    assert(
      exception.getMessage.contains(
        "Failed to parse query due to offending symbol [UNSUPPORTED_DATATYPE]"))

    // Unsupported data type for Spark
    val context2 = new CatalystPlanContext
    val exception2 = intercept[IllegalArgumentException] {
      planTransformer.visit(plan(pplParser, """source=t | eval a = cast(a as time)"""), context2)
    }
    assert(exception2.getMessage == "Unsupported data type for Spark: TIME")
  }

}
