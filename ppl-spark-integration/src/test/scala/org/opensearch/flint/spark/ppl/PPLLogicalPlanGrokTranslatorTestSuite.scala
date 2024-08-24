/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Coalesce, Descending, GreaterThan, Literal, NullsFirst, NullsLast, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.grok.{Grok, GrokCompiler, Match}
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.Map

class PPLLogicalPlanGrokTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test grok email & host expressions") {
    val grokCompiler = GrokCompiler.newInstance
    grokCompiler.registerDefaultPatterns()

    /* Grok pattern to compile, here httpd logs *//* Grok pattern to compile, here httpd logs */
    val grok = grokCompiler.compile(".+@%{HOSTNAME:host}")

    /* Line of log to match *//* Line of log to match */
    val log = "iii@gmail.com"

    val gm = grok.`match`(log)
    val capture: util.Map[String, AnyRef] = gm.capture

    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=accounts | grok email '.+@%{HOSTNAME:host}' | fields email, host",
          isExplain = false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"), Literal("1")))),
      "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("accounts"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok to parse raw logs.") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=apache | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes", false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal("1")))),
      "email")()
    val expectedPlan = Project(
      Seq(emailAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok email expression with filter by age and sort by age field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=accounts | grok email '.+@%{HOSTNAME:host}' | where age > 45 | sort - age | fields age, email, host",
          isExplain = false),
        context)

    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val ageAttribute = UnresolvedAttribute("age")
    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal(1)))),
      "host")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(ageAttribute, emailAttribute, UnresolvedAttribute("host")),
      Sort(
        Seq(SortOrder(ageAttribute, Descending, NullsLast, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(ageAttribute, Literal(45)),
          Project(
            Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
            UnresolvedRelation(Seq("t"))))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok email expression, generate new host field and eval result") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result",
          false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val evalResultAttribute = UnresolvedAttribute("eval_result")

    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal("1")))),
      "host")()

    val evalResultExpression = Alias(Literal(1), "eval_result")()

    val expectedPlan = Project(
      Seq(hostAttribute, evalResultAttribute),
      Project(
        Seq(UnresolvedStar(None), evalResultExpression),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("t")))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }
  
  test("test parse email expressions and group by count host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | grok email '.+@%{HOSTNAME:host}' | stats count() by host", false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal(1)))),
      "host")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      Aggregate(
        Seq(Alias(hostAttribute, "host")()), // Group by 'host'
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
            "count()")(),
          Alias(hostAttribute, "host")()),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("t")))))

    // Compare the logical plans
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test parse email expressions and top count_host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t |  grok email '.+@%{HOSTNAME:host}' | top 1 host", false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      Coalesce(Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal(1)))),
      "host")()

    val sortedPlan = Sort(
      Seq(
        SortOrder(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(hostAttribute), isDistinct = false),
            "count_host")(),
          Descending,
          NullsLast,
          Seq.empty)),
      global = true,
      Aggregate(
        Seq(hostAttribute),
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(hostAttribute), isDistinct = false),
            "count_host")(),
          hostAttribute),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("t")))))
    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      GlobalLimit(Literal(1), LocalLimit(Literal(1), sortedPlan)))
    // Compare the logical plans
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
