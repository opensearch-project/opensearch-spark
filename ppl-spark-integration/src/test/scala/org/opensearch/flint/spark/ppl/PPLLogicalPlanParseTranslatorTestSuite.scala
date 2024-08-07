/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanParseTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test parse email & host expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | fields email, host", false),
        context)
    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test parse email expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<email>.+)' | fields email", false),
        context)
    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
  
  test("test parse email expression, generate new host field and eval result") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | eval eval_result=1 | fields host, eval_result", false),
        context)
    val evalProjectList: Seq[NamedExpression] =
      Seq(UnresolvedStar(None), Alias(Literal(1), "a")(), Alias(Literal(1), "b")())
    val expectedPlan = Project(
      seq(UnresolvedAttribute("c")),
      Project(evalProjectList, UnresolvedRelation(Seq("t"))))
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
