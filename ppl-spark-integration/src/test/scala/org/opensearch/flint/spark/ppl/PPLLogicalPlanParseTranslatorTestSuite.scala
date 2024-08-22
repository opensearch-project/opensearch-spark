/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.ScalaReflection.universe.Star
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Coalesce, GreaterThan, Literal, NamedExpression, NullsFirst, RegExpExtract, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, Sort}
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
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | fields email, host", isExplain = false),
        context)

    val emailAttribute = UnresolvedAttribute("email")
    val hostAttribute = UnresolvedAttribute("host")
    val hostExpression = Alias(
      Coalesce(
        Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal("1")))), "host")()
    val expectedPlan = Project(
      Seq(emailAttribute, hostAttribute),
      Project(
        Seq(emailAttribute, hostExpression),
        UnresolvedRelation(Seq("t"))
      ))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email expression") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<email>.+)' | fields email", false),
        context)
        
    val emailAttribute = UnresolvedAttribute("email")
    val hostExpression = Alias(
      Coalesce(
        Seq(RegExpExtract(emailAttribute, Literal(".+@(.+)"), Literal("1")))), "email")()
    val expectedPlan = Project(
      Seq(emailAttribute),
      Project(
        Seq(emailAttribute, hostExpression),
        UnresolvedRelation(Seq("t"))
      ))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email expression, generate new host field and eval result") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse email '.+@(?<host>.+)' | eval eval_result=1 | fields host, eval_result", false),
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
          Seq(emailAttribute, hostExpression),
          UnresolvedRelation(Seq("t"))
        )
      )
    )
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test parse email & host expressions including cast and sort commands") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | parse address '(?<streetNumber>\\d+) (?<street>.+)' | where streetNumber > 500 | sort num(streetNumber) | fields streetNumber, street", false),
        context)

    val addressAttribute = UnresolvedAttribute("address")
    val streetNumberAttribute = UnresolvedAttribute("streetNumber")
    val streetAttribute = UnresolvedAttribute("street")

    val streetNumberExpression = Alias(
      Coalesce(Seq(RegExpExtract(addressAttribute, Literal("(\\d+) (.+)"), Literal("1")))),
      "streetNumber"
    )()

    val streetExpression = Alias(
      Coalesce(Seq(RegExpExtract(addressAttribute, Literal("(\\d+) (.+)"), Literal("2")))),
      "street")()

    val expectedPlan = Project(
      Seq(streetNumberAttribute, streetAttribute),
      Sort(
        Seq(SortOrder(streetNumberAttribute, Ascending, NullsFirst, Seq.empty)),
        global = true,
        Filter(
          GreaterThan(streetNumberAttribute, Literal(500)),
          Project(
            Seq(addressAttribute, streetNumberExpression, streetExpression),
            UnresolvedRelation(Seq("t"))
          )
        )
      )
    )

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }}
