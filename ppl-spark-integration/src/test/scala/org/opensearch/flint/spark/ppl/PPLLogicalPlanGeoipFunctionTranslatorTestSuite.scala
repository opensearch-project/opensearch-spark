/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.expression.function.BuiltinFunctionName.{IP_TO_INT, IS_IPV4}
import org.opensearch.sql.expression.function.SerializableUdf.visit
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, CreateNamedStruct, EqualTo, Expression, GreaterThanOrEqual, LessThan, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Join, JoinHint, LogicalPlan, Project, SubqueryAlias}

class PPLLogicalPlanGeoipFunctionTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  private def getGeoIpQueryPlan(
      ipAddress: UnresolvedAttribute,
      left: LogicalPlan,
      right: LogicalPlan,
      projectionProperties: Alias): LogicalPlan = {
    val joinPlan = getJoinPlan(ipAddress, left, right)
    getProjection(joinPlan, projectionProperties)
  }

  private def getJoinPlan(
      ipAddress: UnresolvedAttribute,
      left: LogicalPlan,
      right: LogicalPlan): LogicalPlan = {
    val is_ipv4 = visit(IS_IPV4, util.List.of[Expression](ipAddress))
    val ip_to_int = visit(IP_TO_INT, util.List.of[Expression](ipAddress))

    val t1 = SubqueryAlias("t1", left)
    val t2 = SubqueryAlias("t2", right)

    val joinCondition = And(
      And(
        GreaterThanOrEqual(ip_to_int, UnresolvedAttribute("t2.ip_range_start")),
        LessThan(ip_to_int, UnresolvedAttribute("t2.ip_range_end"))),
      EqualTo(is_ipv4, UnresolvedAttribute("t2.ipv4")))
    Join(t1, t2, LeftOuter, Some(joinCondition), JoinHint.NONE)
  }

  private def getProjection(joinPlan: LogicalPlan, projectionProperties: Alias): LogicalPlan = {
    val projection = Project(Seq(UnresolvedStar(None), projectionProperties), joinPlan)
    val dropList = Seq(
      "t2.country_iso_code",
      "t2.country_name",
      "t2.continent_name",
      "t2.region_iso_code",
      "t2.region_name",
      "t2.city_name",
      "t2.time_zone",
      "t2.location",
      "t2.cidr",
      "t2.ip_range_start",
      "t2.ip_range_end",
      "t2.ipv4").map(UnresolvedAttribute(_))
    DataFrameDropColumns(dropList, projection)
  }

  test("test geoip function - only ip_address provided") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source = users | eval a = geoip(ip_address)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("users"))
    val geoTable = UnresolvedRelation(seq("geoip"))

    val projectionStruct = CreateNamedStruct(
      Seq(
        Literal("country_iso_code"),
        UnresolvedAttribute("t2.country_iso_code"),
        Literal("country_name"),
        UnresolvedAttribute("t2.country_name"),
        Literal("continent_name"),
        UnresolvedAttribute("t2.continent_name"),
        Literal("region_iso_code"),
        UnresolvedAttribute("t2.region_iso_code"),
        Literal("region_name"),
        UnresolvedAttribute("t2.region_name"),
        Literal("city_name"),
        UnresolvedAttribute("t2.city_name"),
        Literal("time_zone"),
        UnresolvedAttribute("t2.time_zone"),
        Literal("location"),
        UnresolvedAttribute("t2.location")))
    val structProjection = Alias(projectionStruct, "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - source has same name as join alias") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t1 | eval a = geoip(ip_address, country_name)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("t1"))
    val geoTable = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - ipAddress col exist in geoip table") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t1 | eval a = geoip(cidr, country_name)"),
        context)

    val ipAddress = UnresolvedAttribute("cidr")
    val sourceTable = UnresolvedRelation(seq("t1"))
    val geoTable = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - duplicate parameters") {
    val context = new CatalystPlanContext

    val exception = intercept[IllegalStateException] {
      planTransformer.visit(
        plan(pplParser, "source=t1 | eval a = geoip(cidr, country_name, country_name)"),
        context)
    }

    assert(exception.getMessage.contains("Duplicate attribute in GEOIP attribute list"))
  }

  test("test geoip function - one property provided") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=users | eval a = geoip(ip_address, country_name)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("users"))
    val geoTable = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - multiple properties provided") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=users | eval a = geoip(ip_address,country_name,location)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("users"))
    val geoTable = UnresolvedRelation(seq("geoip"))
    val projectionStruct = CreateNamedStruct(
      Seq(
        Literal("country_name"),
        UnresolvedAttribute("t2.country_name"),
        Literal("location"),
        UnresolvedAttribute("t2.location")))
    val structProjection = Alias(projectionStruct, "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - multiple geoip calls") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = geoip(ip_address, country_iso_code), b = geoip(ip_address, region_iso_code)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("t"))
    val geoTable = UnresolvedRelation(seq("geoip"))

    val structProjectionA = Alias(UnresolvedAttribute("t2.country_iso_code"), "a")()
    val colAPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjectionA)

    val structProjectionB = Alias(UnresolvedAttribute("t2.region_iso_code"), "b")()
    val colBPlan = getGeoIpQueryPlan(ipAddress, colAPlan, geoTable, structProjectionB)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), colBPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - other eval function used between geoip") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = geoip(ip_address, time_zone), b = rand(), c = geoip(ip_address, region_name)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("t"))
    val geoTable = UnresolvedRelation(seq("geoip"))

    val structProjectionA = Alias(UnresolvedAttribute("t2.time_zone"), "a")()
    val colAPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjectionA)

    val structProjectionC = Alias(UnresolvedAttribute("t2.region_name"), "c")()
    val colCPlan = getGeoIpQueryPlan(ipAddress, colAPlan, geoTable, structProjectionC)

    val randProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("rand", Seq.empty, isDistinct = false), "b")())
    val colBPlan = Project(randProjectList, colCPlan)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), colBPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - other eval function used before geoip") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | eval a = rand(), b = geoip(ip_address, city_name)"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("t"))
    val geoTable = UnresolvedRelation(seq("geoip"))

    val structProjectionB = Alias(UnresolvedAttribute("t2.city_name"), "b")()
    val colBPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjectionB)

    val randProjectList: Seq[NamedExpression] = Seq(
      UnresolvedStar(None),
      Alias(UnresolvedFunction("rand", Seq.empty, isDistinct = false), "a")())
    val colAPlan = Project(randProjectList, colBPlan)

    val expectedPlan = Project(Seq(UnresolvedStar(None)), colAPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip function - projection on evaluated field") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=users | eval a = geoip(ip_address, country_name) | fields a"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("users"))
    val geoTable = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()

    val geoIpPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjection)
    val expectedPlan = Project(Seq(UnresolvedAttribute("a")), geoIpPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }

  test("test geoip with partial projection on evaluated fields") {
    val context = new CatalystPlanContext

    val logPlan =
      planTransformer.visit(
        plan(
          pplParser,
          "source=t | eval a = geoip(ip_address, country_iso_code), b = geoip(ip_address, region_iso_code) | fields b"),
        context)

    val ipAddress = UnresolvedAttribute("ip_address")
    val sourceTable = UnresolvedRelation(seq("t"))
    val geoTable = UnresolvedRelation(seq("geoip"))

    val structProjectionA = Alias(UnresolvedAttribute("t2.country_iso_code"), "a")()
    val colAPlan = getGeoIpQueryPlan(ipAddress, sourceTable, geoTable, structProjectionA)

    val structProjectionB = Alias(UnresolvedAttribute("t2.region_iso_code"), "b")()
    val colBPlan = getGeoIpQueryPlan(ipAddress, colAPlan, geoTable, structProjectionB)

    val expectedPlan = Project(Seq(UnresolvedAttribute("b")), colBPlan)

    comparePlans(expectedPlan, logPlan, checkAnalysis = false)
  }
}
