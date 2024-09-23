/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util
import java.util.Map

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.grok.{Grok, GrokCompiler, Match}
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Coalesce, Descending, GreaterThan, Literal, NullsFirst, NullsLast, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

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

    /* Grok pattern to compile, here httpd logs */ /* Grok pattern to compile, here httpd logs */
    val grok = grokCompiler.compile(".+@%{HOSTNAME:host}")

    /* Line of log to match */ /* Line of log to match */
    val log = "iii@gmail.com"

    val gm = grok.`match`(log)
    val capture: util.Map[String, AnyRef] = gm.capture

    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=accounts | grok email '.+@%{HOSTNAME:host}' | fields email, host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal("1")),
      "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("accounts"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok to grok raw logs.") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | grok message '%{COMMONAPACHELOG}' | fields COMMONAPACHELOG, timestamp, response, bytes"),
        context)

    val messageAttribute = UnresolvedAttribute("message")
    val logAttribute = UnresolvedAttribute("COMMONAPACHELOG")
    val timestampAttribute = UnresolvedAttribute("timestamp")
    val responseAttribute = UnresolvedAttribute("response")
    val bytesAttribute = UnresolvedAttribute("bytes")
    // scalastyle:off
    val expectedRegExp =
      "(?<name0>(?<name1>(?:(?<name2>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))|(?<name3>(?:(?<name4>((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?)|(?<name5>(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])))))) (?<name6>(?<name7>[a-zA-Z0-9._-]+)) (?<name8>(?<name9>[a-zA-Z0-9._-]+)) \\[(?<name10>(?<name11>(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9]))/(?<name12>\\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\\b)/(?<name13>(?>\\d\\d){1,2}):(?<name14>(?!<[0-9])(?<name15>(?:2[0123]|[01]?[0-9])):(?<name16>(?:[0-5][0-9]))(?::(?<name17>(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)))(?![0-9])) (?<name18>(?:[+-]?(?:[0-9]+))))\\] \"(?:(?<name19>\\b\\w+\\b) (?<name20>\\S+)(?: HTTP/(?<name21>(?:(?<name22>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))))?|(?<name23>.*?))\" (?<name24>(?:(?<name25>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))) (?:(?<name26>(?:(?<name27>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+))))))|-))"
    // scalastyle:on

    val COMMONAPACHELOG = Alias(
      RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("1")),
      "COMMONAPACHELOG")()
    val timestamp = Alias(
      RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("11")),
      "timestamp")()
    val response =
      Alias(RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("25")), "response")()
    val bytes =
      Alias(RegExpExtract(messageAttribute, Literal(expectedRegExp), Literal("27")), "bytes")()
    val expectedPlan = Project(
      Seq(logAttribute, timestampAttribute, responseAttribute, bytesAttribute),
      Project(
        Seq(messageAttribute, COMMONAPACHELOG, timestamp, response, bytes, UnresolvedStar(None)),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok email expression with filter by age and sort by age field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=accounts | grok email '.+@%{HOSTNAME:host}' | where age > 45 | sort - age | fields age, email, host"),
        context)

    // Define the expected logical plan
    val emailAttribute = UnresolvedAttribute("email")
    val ageAttribute = UnresolvedAttribute("age")
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal(1)),
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
            UnresolvedRelation(Seq("accounts"))))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok email expression, generate new host field and eval result") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=accounts | grok email '.+@%{HOSTNAME:host}' | eval eval_result=1 | fields host, eval_result"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val evalResultAttribute = UnresolvedAttribute("eval_result")

    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal("1")),
      "host")()

    val evalResultExpression = Alias(Literal(1), "eval_result")()

    val expectedPlan = Project(
      Seq(hostAttribute, evalResultAttribute),
      Project(
        Seq(UnresolvedStar(None), evalResultExpression),
        Project(
          Seq(emailAttribute, hostExpression, UnresolvedStar(None)),
          UnresolvedRelation(Seq("accounts")))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test grok email expressions and group by count host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | grok email '.+@%{HOSTNAME:host}' | stats count() by host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal(1)),
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

  test("test grok email expressions and top count_host ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t |  grok email '.+@%{HOSTNAME:host}' | top 1 host"),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      RegExpExtract(
        emailAttribute,
        Literal(
          ".+@(?<name0>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b))"),
        Literal(1)),
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

  test("test grok address expressions with 2 fields identifies ") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=accounts | grok street_address '%{NUMBER} %{GREEDYDATA:address}' | fields address "),
        context)

    val street_addressAttribute = UnresolvedAttribute("street_address")
    val addressAttribute = UnresolvedAttribute("address")
    val addressExpression = Alias(
      RegExpExtract(
        street_addressAttribute,
        Literal(
          "(?<name0>(?:(?<name1>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))) (?<name2>.*)"),
        Literal("3")),
      "address")()
    val expectedPlan = Project(
      Seq(addressAttribute),
      Project(
        Seq(street_addressAttribute, addressExpression, UnresolvedStar(None)),
        UnresolvedRelation(Seq("accounts"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }
}
