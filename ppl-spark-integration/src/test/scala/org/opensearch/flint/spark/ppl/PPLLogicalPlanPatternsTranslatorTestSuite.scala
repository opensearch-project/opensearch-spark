/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, GreaterThan, Literal, NullsLast, RegExpExtract, RegExpReplace, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class PPLLogicalPlanPatternsTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test patterns email & host expressions") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=accounts | patterns email | fields email, patterns_field ",
          isExplain = false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val patterns_field = UnresolvedAttribute("patterns_field")
    val hostExpression = Alias(
      RegExpReplace(emailAttribute, Literal("[a-zA-Z0-9]"), Literal("")),
      "patterns_field")()
    val expectedPlan = Project(
      Seq(emailAttribute, patterns_field),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("accounts"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test(
    "test patterns extract punctuations from a raw log field using user defined patterns and a new field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | fields message, no_numbers",
          false),
        context)

    val emailAttribute = UnresolvedAttribute("message")
    val patterns_field = UnresolvedAttribute("no_numbers")
    val hostExpression =
      Alias(RegExpReplace(emailAttribute, Literal("[0-9]"), Literal("")), "no_numbers")()
    val expectedPlan = Project(
      Seq(emailAttribute, patterns_field),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("apache"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))

  }

  test("test patterns email & host expressions with filter by age and sort by age field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=accounts | patterns email | where age > 45 | sort - age | fields email, patterns_field",
          isExplain = false),
        context)

    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val patterns_fieldAttribute = UnresolvedAttribute("patterns_field")
    val ageAttribute = UnresolvedAttribute("age")
    val hostExpression = Alias(
      RegExpReplace(emailAttribute, Literal("[a-zA-Z0-9]"), Literal("")),
      "patterns_field")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(emailAttribute, patterns_fieldAttribute),
      Sort(
        Seq(SortOrder(ageAttribute, Descending, NullsLast, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(ageAttribute, Literal(45)),
          Project(
            Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
            UnresolvedRelation(Seq("accounts"))))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test patterns email expressions and group by count host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | stats count() by no_numbers",
          false),
        context)

    val messageAttribute = UnresolvedAttribute("message")
    val noNumbersAttribute = UnresolvedAttribute("no_numbers")
    val hostExpression =
      Alias(RegExpReplace(messageAttribute, Literal("[0-9]"), Literal("")), "no_numbers")()

    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      Aggregate(
        Seq(Alias(noNumbersAttribute, "no_numbers")()), // Group by 'no_numbers'
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(UnresolvedStar(None)), isDistinct = false),
            "count()")(),
          Alias(noNumbersAttribute, "no_numbers")()),
        Project(
          Seq(messageAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("apache")))))

    // Compare the logical plans
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test patterns email expressions and top count_host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | top 1 no_numbers",
          false),
        context)

    val messageAttribute = UnresolvedAttribute("message")
    val noNumbersAttribute = UnresolvedAttribute("no_numbers")
    val hostExpression =
      Alias(RegExpReplace(messageAttribute, Literal("[0-9]"), Literal("")), "no_numbers")()

    val sortedPlan = Sort(
      Seq(
        SortOrder(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(noNumbersAttribute), isDistinct = false),
            "count_no_numbers")(),
          Descending,
          NullsLast,
          Seq.empty)),
      global = true,
      Aggregate(
        Seq(noNumbersAttribute),
        Seq(
          Alias(
            UnresolvedFunction(Seq("COUNT"), Seq(noNumbersAttribute), isDistinct = false),
            "count_no_numbers")(),
          noNumbersAttribute),
        Project(
          Seq(messageAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("apache")))))
    // Define the corrected expected plan
    val expectedPlan = Project(
      Seq(UnresolvedStar(None)), // Matches the '*' in the Project
      GlobalLimit(Literal(1), LocalLimit(Literal(1), sortedPlan)))
    // Compare the logical plans
    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
