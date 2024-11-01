/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, CaseWhen, Descending, EqualTo, GreaterThan, Literal, NullsFirst, NullsLast, RegExpExtract, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.DataTypes
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.expression.function.SerializableUdf
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanParseCidrmatchTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  
  test("test cidrmatch for ipv4 for 192.168.1.0/24") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t  | where isV6 = false and isValid = true and cidrmatch(ipAddress, '192.168.1.0/24')"),
        context)

    val ipAddress = UnresolvedAttribute("ipAddress")
    val cidrExpression = Literal("192.168.1.0/24")

    val filterIpv6 = EqualTo(UnresolvedAttribute("isV6"), Literal(false))
    val filterIsValid = EqualTo(UnresolvedAttribute("isValid"), Literal(true))
    val cidr = ScalaUDF(SerializableUdf.cidrFunction,
                        DataTypes.BooleanType,
                        seq(ipAddress, cidrExpression),
                        seq(),
                        Option.empty,
                        Option.apply("cidr"), false, true)

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      Filter(And(And(filterIpv6, filterIsValid), cidr),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test cidrmatch for ipv6 for 2003:db8::/32") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t  | where isV6 = true and isValid = false and cidrmatch(ipAddress, '2003:db8::/32')"),
        context)

    val ipAddress = UnresolvedAttribute("ipAddress")
    val cidrExpression = Literal("2003:db8::/32")

    val filterIpv6 = EqualTo(UnresolvedAttribute("isV6"), Literal(true))
    val filterIsValid = EqualTo(UnresolvedAttribute("isValid"), Literal(false))
    val cidr = ScalaUDF(SerializableUdf.cidrFunction,
                        DataTypes.BooleanType,
                        seq(ipAddress, cidrExpression),
                        seq(),
                        Option.empty,
                        Option.apply("cidr"), false, true)

    val expectedPlan = Project(
      Seq(UnresolvedStar(None)),
      Filter(And(And(filterIpv6, filterIsValid), cidr),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

  test("test cidrmatch for ipv6 for 2003:db8::/32 with ip field projected" ) {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t  | where isV6 = true and cidrmatch(ipAddress, '2003:db8::/32') | fields ip"),
        context)

    val ipAddress = UnresolvedAttribute("ipAddress")
    val cidrExpression = Literal("2003:db8::/32")

    val filterIpv6 = EqualTo(UnresolvedAttribute("isV6"), Literal(true))
    val cidr = ScalaUDF(SerializableUdf.cidrFunction,
                        DataTypes.BooleanType,
                        seq(ipAddress, cidrExpression),
                        seq(),
                        Option.empty,
                        Option.apply("cidr"), false, true)

    val expectedPlan = Project(
      Seq(UnresolvedAttribute("ip")),
      Filter(And(filterIpv6, cidr),
        UnresolvedRelation(Seq("t"))))
    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }
  
  test("test cidrmatch for ipv6 for 2003:db8::/32 with ip field bool respond for each ip" ) {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t  | where isV6 = true | eval inRange = case(cidrmatch(ipAddress, '2003:db8::/32'), 'in' else 'out') | fields ip, inRange"),
        context)

    val ipAddress = UnresolvedAttribute("ipAddress")
    val cidrExpression = Literal("2003:db8::/32")

    val filterIpv6 = EqualTo(UnresolvedAttribute("isV6"), Literal(true))
    val filterClause = Filter(filterIpv6, UnresolvedRelation(Seq("t")))
    val cidr = ScalaUDF(SerializableUdf.cidrFunction,
                        DataTypes.BooleanType,
                        seq(ipAddress, cidrExpression),
                        seq(),
                        Option.empty,
                        Option.apply("cidr"), false, true)

    val equalTo = EqualTo(Literal(true), cidr)
    val caseFunction = CaseWhen(Seq((equalTo, Literal("in"))), Literal("out"))
    val aliasStatusCategory = Alias(caseFunction, "inRange")()
    val evalProjectList = Seq(UnresolvedStar(None), aliasStatusCategory)
    val evalProject = Project(evalProjectList, filterClause)

    val expectedPlan = Project(
      Seq(UnresolvedAttribute("ip"), UnresolvedAttribute("inRange")), evalProject)

    assert(compareByString(expectedPlan) === compareByString(logPlan))
  }

}
