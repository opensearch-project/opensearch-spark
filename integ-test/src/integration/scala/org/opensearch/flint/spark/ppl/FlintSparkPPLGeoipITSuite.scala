/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import java.util

import org.opensearch.sql.expression.function.BuiltinFunctionName.IP_TO_INT
import org.opensearch.sql.expression.function.BuiltinFunctionName.IS_IPV4
import org.opensearch.sql.expression.function.SerializableUdf.visit
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, CreateNamedStruct, EqualTo, Expression, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.{DataFrameDropColumns, Filter, Join, JoinHint, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLGeoipITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.flint_ppl_test"
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createGeoIpTestTable(testTable)
    createGeoIpTable()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

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

  test("test geoip with no parameters") {
    val frame = sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip) | fields ip, a
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()

    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row(
        "66.249.157.90",
        Row(
          "JM",
          "Jamaica",
          "North America",
          "14",
          "Saint Catherine Parish",
          "Portmore",
          "America/Jamaica",
          "17.9686,-76.8827")),
      Row(
        "2a09:bac2:19f8:2ac3::",
        Row(
          "CA",
          "Canada",
          "North America",
          "PE",
          "Prince Edward Island",
          "Charlottetown",
          "America/Halifax",
          "46.2396,-63.1355")))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Compare the logical plans
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sourceTable: LogicalPlan = Filter(
      EqualTo(UnresolvedAttribute("isValid"), Literal(true)),
      UnresolvedRelation(testTable.split("\\.").toSeq))
    val geoTable: LogicalPlan = UnresolvedRelation(seq("geoip"))
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
    val geoIpPlan =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), sourceTable, geoTable, structProjection)
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("ip"), UnresolvedAttribute("a")), geoIpPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test geoip with one parameters") {
    val frame = sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip, country_name) | fields ip, a
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("66.249.157.90", "Jamaica"), Row("2a09:bac2:19f8:2ac3::", "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Compare the logical plans
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sourceTable: LogicalPlan = Filter(
      EqualTo(UnresolvedAttribute("isValid"), Literal(true)),
      UnresolvedRelation(testTable.split("\\.").toSeq))
    val geoTable: LogicalPlan = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()
    val geoIpPlan =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), sourceTable, geoTable, structProjection)
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("ip"), UnresolvedAttribute("a")), geoIpPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test geoip with multiple parameters") {
    val frame = sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip, country_name, city_name) | fields ip, a
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] = Array(
      Row("66.249.157.90", Row("Jamaica", "Portmore")),
      Row("2a09:bac2:19f8:2ac3::", Row("Canada", "Charlottetown")))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Compare the logical plans
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sourceTable: LogicalPlan = Filter(
      EqualTo(UnresolvedAttribute("isValid"), Literal(true)),
      UnresolvedRelation(testTable.split("\\.").toSeq))
    val geoTable: LogicalPlan = UnresolvedRelation(seq("geoip"))
    val projectionStruct = CreateNamedStruct(
      Seq(
        Literal("country_name"),
        UnresolvedAttribute("t2.country_name"),
        Literal("city_name"),
        UnresolvedAttribute("t2.city_name")))
    val structProjection = Alias(projectionStruct, "a")()
    val geoIpPlan =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), sourceTable, geoTable, structProjection)
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("ip"), UnresolvedAttribute("a")), geoIpPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test geoip with partial projection on evaluated fields") {
    val frame = sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip, city_name), b = geoip(ip, country_name) | fields ip, b
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("66.249.157.90", "Jamaica"), Row("2a09:bac2:19f8:2ac3::", "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Compare the logical plans
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sourceTable: LogicalPlan = Filter(
      EqualTo(UnresolvedAttribute("isValid"), Literal(true)),
      UnresolvedRelation(testTable.split("\\.").toSeq))
    val geoTable: LogicalPlan = UnresolvedRelation(seq("geoip"))

    val structProjectionA = Alias(UnresolvedAttribute("t2.city_name"), "a")()
    val geoIpPlanA =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), sourceTable, geoTable, structProjectionA)

    val structProjectionB = Alias(UnresolvedAttribute("t2.country_name"), "b")()
    val geoIpPlanB =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), geoIpPlanA, geoTable, structProjectionB)

    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("ip"), UnresolvedAttribute("b")), geoIpPlanB)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test geoip with projection on field that exists in both source and geoip table") {
    val frame = sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip, country_name) | fields ipv4, a
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("66.249.157.90", "Jamaica"), Row("Given IPv6 is not mapped to IPv4", "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    // Compare the logical plans
    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val sourceTable: LogicalPlan = Filter(
      EqualTo(UnresolvedAttribute("isValid"), Literal(true)),
      UnresolvedRelation(testTable.split("\\.").toSeq))
    val geoTable: LogicalPlan = UnresolvedRelation(seq("geoip"))
    val structProjection = Alias(UnresolvedAttribute("t2.country_name"), "a")()
    val geoIpPlan =
      getGeoIpQueryPlan(UnresolvedAttribute("ip"), sourceTable, geoTable, structProjection)
    val expectedPlan: LogicalPlan =
      Project(Seq(UnresolvedAttribute("ipv4"), UnresolvedAttribute("a")), geoIpPlan)

    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test geoip with invalid parameter") {
    assertThrows[ParseException](sql(s"""
         | source = $testTable | where isValid = true | eval a = geoip(ip, invalid_param) | fields ip, a
         | """.stripMargin))
  }

  test("test geoip with invalid ip address provided") {
    val frame = sql(s"""
         | source = $testTable | eval a = geoip(ip) | fields ip, a
         | """.stripMargin)

    // Retrieve the results
    assertThrows[SparkException](frame.collect())
  }
}
