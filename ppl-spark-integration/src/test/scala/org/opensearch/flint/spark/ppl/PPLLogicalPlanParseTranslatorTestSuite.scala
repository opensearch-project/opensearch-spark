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
import org.apache.spark.sql.catalyst.ScalaReflection.universe.Star
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Cast, Coalesce, Descending, GreaterThan, Literal, NamedExpression, NullsFirst, NullsLast, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, LocalLimit, Project, Sort}
import org.apache.spark.sql.types.IntegerType

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
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | fields email, host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal("1")), "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<email>.+)' | fields email"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<email>.+)"), Literal("1")), "email")()
    val expectedPlan = Project(
      Seq(emailAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email expression with filter by age and sort by age field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source = t | parse email '.+@(?<host>.+)' | where age > 45 | sort - age | fields age, email, host"),
        context)

    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val ageAttribute = UnresolvedAttribute("age")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

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

  test("test parse email expression, generate new host field and eval result") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | parse email '.+@(?<host>.+)' | eval eval_result=1 | fields host, eval_result"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val evalResultAttribute = UnresolvedAttribute("eval_result")

    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal("1")), "host")()

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

  test("test parse street number & street expressions including cast and sort commands") {
    val context = new CatalystPlanContext

    // TODO #963: Implement 'num', 'str', and 'ip' sort syntax
    val query =
      "source=t" +
        " | parse address '(?<streetNumber>\\d+) (?<street>.+)'" +
        " | eval streetNumberInt = cast(streetNumber as integer)" +
        " | where streetNumberInt > 500" +
        " | sort streetNumberInt" +
        " | fields streetNumber, street"

    val logPlan = planTransformer.visit(plan(pplParser, query), context)

    val addressAttribute = UnresolvedAttribute("address")
    val streetNumberAttribute = UnresolvedAttribute("streetNumber")
    val streetAttribute = UnresolvedAttribute("street")
    val streetNumberIntAttribute = UnresolvedAttribute("streetNumberInt")

    val regexLiteral = Literal("(?<streetNumber>\\d+) (?<street>.+)")
    val streetNumberExpression =
      Alias(RegExpExtract(addressAttribute, regexLiteral, Literal("1")), "streetNumber")()
    val streetExpression =
      Alias(RegExpExtract(addressAttribute, regexLiteral, Literal("2")), "street")()

    val castExpression = Cast(streetNumberAttribute, IntegerType)

    val expectedPlan = Project(
      Seq(streetNumberAttribute, streetAttribute),
      Sort(
        Seq(SortOrder(streetNumberIntAttribute, Ascending, NullsFirst, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(streetNumberIntAttribute, Literal(500)),
          Project(
            Seq(UnresolvedStar(None), Alias(castExpression, "streetNumberInt")()),
            Project(
              Seq(
                addressAttribute,
                streetNumberExpression,
                streetExpression,
                UnresolvedStar(None)),
              UnresolvedRelation(Seq("t")))))))

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email expressions and group by count host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | stats count() by host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

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
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | top 1 host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression =
      Alias(RegExpExtract(emailAttribute, Literal(".+@(?<host>.+)"), Literal(1)), "host")()

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
