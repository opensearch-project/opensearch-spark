/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, GreaterThan, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

class FlintSparkPPLFileSourceRelationITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  /** Test table and index name */
  private val testTable = "flint_ppl_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("test csv file source relation") {
    val frame = sql(s"""
         | file = $testTable ( "integ-test/src/integration/resources/opensearch_dashboards_sample_data_flights.csv" )
         | | where Cancelled = false AND DistanceMiles > 0
         | | fields FlightNum, DistanceMiles, FlightTimeMin, OriginWeather, AvgTicketPrice, Carrier, FlightDelayType, timestamp
         | | stats avg(AvgTicketPrice) as avg_price_by_weather by OriginWeather
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(768.1694081139743, "Clear"),
      Row(826.1736096641965, "Cloudy"),
      Row(600.4401843290168, "Rain"),
      Row(512.8754006548804, "Sunny"),
      Row(632.8519057524543, "Thunder & Lightning"),
      Row(626.7242371194318, "Damaging Wind"))
    implicit val oneColRowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val userSpecifiedSchema = new StructType()
      .add("FlightNum", StringType)
      .add("Origin", StringType)
      .add("FlightDelay", BooleanType)
      .add("DistanceMiles", DoubleType)
      .add("FlightTimeMin", DoubleType)
      .add("OriginWeather", StringType)
      .add("dayOfWeek", IntegerType)
      .add("AvgTicketPrice", DoubleType)
      .add("Carrier", StringType)
      .add("FlightDelayMin", IntegerType)
      .add("OriginRegion", StringType)
      .add("FlightDelayType", StringType)
      .add("DestAirportID", StringType)
      .add("Dest", StringType)
      .add("FlightTimeHour", DoubleType)
      .add("Cancelled", BooleanType)
      .add("DistanceKilometers", DoubleType)
      .add("OriginCityName", StringType)
      .add("DestWeather", StringType)
      .add("OriginCountry", StringType)
      .add("DestCountry", StringType)
      .add("DestRegion", StringType)
      .add("OriginAirportID", StringType)
      .add("DestCityName", StringType)
      .add("timestamp", TimestampType)
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "csv",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val filterExpr = And(
      EqualTo(UnresolvedAttribute("Cancelled"), Literal(false)),
      GreaterThan(UnresolvedAttribute("DistanceMiles"), Literal(0)))
    val filter = Filter(filterExpr, relation)
    val project = Project(
      Seq(
        UnresolvedAttribute("FlightNum"),
        UnresolvedAttribute("DistanceMiles"),
        UnresolvedAttribute("FlightTimeMin"),
        UnresolvedAttribute("OriginWeather"),
        UnresolvedAttribute("AvgTicketPrice"),
        UnresolvedAttribute("Carrier"),
        UnresolvedAttribute("FlightDelayType"),
        UnresolvedAttribute("timestamp")),
      filter)
    val weatherAlias = Alias(UnresolvedAttribute("OriginWeather"), "OriginWeather")()
    val groupByAttributes = Seq(weatherAlias)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("AVG"),
          Seq(UnresolvedAttribute("AvgTicketPrice")),
          isDistinct = false),
        "avg_price_by_weather")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, weatherAlias), project)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test csv file source relation with compression codec") {
    val frame = sql(s"""
         | file = $testTable ( "integ-test/src/integration/resources/opensearch_dashboards_sample_data_flights.csv.gz" )
         | | where Cancelled = false AND DistanceMiles > 0
         | | fields FlightNum, DistanceMiles, FlightTimeMin, OriginWeather, AvgTicketPrice, Carrier, FlightDelayType, timestamp
         | | stats avg(AvgTicketPrice) as avg_price_by_weather by OriginWeather
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(768.1694081139743, "Clear"),
      Row(826.1736096641965, "Cloudy"),
      Row(600.4401843290168, "Rain"),
      Row(512.8754006548804, "Sunny"),
      Row(632.8519057524543, "Thunder & Lightning"),
      Row(626.7242371194318, "Damaging Wind"))
    implicit val oneColRowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val userSpecifiedSchema = new StructType()
      .add("FlightNum", StringType)
      .add("Origin", StringType)
      .add("FlightDelay", BooleanType)
      .add("DistanceMiles", DoubleType)
      .add("FlightTimeMin", DoubleType)
      .add("OriginWeather", StringType)
      .add("dayOfWeek", IntegerType)
      .add("AvgTicketPrice", DoubleType)
      .add("Carrier", StringType)
      .add("FlightDelayMin", IntegerType)
      .add("OriginRegion", StringType)
      .add("FlightDelayType", StringType)
      .add("DestAirportID", StringType)
      .add("Dest", StringType)
      .add("FlightTimeHour", DoubleType)
      .add("Cancelled", BooleanType)
      .add("DistanceKilometers", DoubleType)
      .add("OriginCityName", StringType)
      .add("DestWeather", StringType)
      .add("OriginCountry", StringType)
      .add("DestCountry", StringType)
      .add("DestRegion", StringType)
      .add("OriginAirportID", StringType)
      .add("DestCityName", StringType)
      .add("timestamp", TimestampType)
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "csv",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val filterExpr = And(
      EqualTo(UnresolvedAttribute("Cancelled"), Literal(false)),
      GreaterThan(UnresolvedAttribute("DistanceMiles"), Literal(0)))
    val filter = Filter(filterExpr, relation)
    val project = Project(
      Seq(
        UnresolvedAttribute("FlightNum"),
        UnresolvedAttribute("DistanceMiles"),
        UnresolvedAttribute("FlightTimeMin"),
        UnresolvedAttribute("OriginWeather"),
        UnresolvedAttribute("AvgTicketPrice"),
        UnresolvedAttribute("Carrier"),
        UnresolvedAttribute("FlightDelayType"),
        UnresolvedAttribute("timestamp")),
      filter)
    val weatherAlias = Alias(UnresolvedAttribute("OriginWeather"), "OriginWeather")()
    val groupByAttributes = Seq(weatherAlias)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("AVG"),
          Seq(UnresolvedAttribute("AvgTicketPrice")),
          isDistinct = false),
        "avg_price_by_weather")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, weatherAlias), project)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test csv file source relation with filter") {
    val frame = sql(s"""
         | file = $testTable ( "integ-test/src/integration/resources/opensearch_dashboards_sample_data_flights.csv" )
         |    Cancelled = false AND DistanceMiles > 0
         | | fields FlightNum, DistanceMiles, FlightTimeMin, OriginWeather, AvgTicketPrice, Carrier, FlightDelayType, timestamp
         | | stats avg(AvgTicketPrice) as avg_price_by_weather by OriginWeather
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(
      Row(768.1694081139743, "Clear"),
      Row(826.1736096641965, "Cloudy"),
      Row(600.4401843290168, "Rain"),
      Row(512.8754006548804, "Sunny"),
      Row(632.8519057524543, "Thunder & Lightning"),
      Row(626.7242371194318, "Damaging Wind"))
    implicit val oneColRowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val userSpecifiedSchema = new StructType()
      .add("FlightNum", StringType)
      .add("Origin", StringType)
      .add("FlightDelay", BooleanType)
      .add("DistanceMiles", DoubleType)
      .add("FlightTimeMin", DoubleType)
      .add("OriginWeather", StringType)
      .add("dayOfWeek", IntegerType)
      .add("AvgTicketPrice", DoubleType)
      .add("Carrier", StringType)
      .add("FlightDelayMin", IntegerType)
      .add("OriginRegion", StringType)
      .add("FlightDelayType", StringType)
      .add("DestAirportID", StringType)
      .add("Dest", StringType)
      .add("FlightTimeHour", DoubleType)
      .add("Cancelled", BooleanType)
      .add("DistanceKilometers", DoubleType)
      .add("OriginCityName", StringType)
      .add("DestWeather", StringType)
      .add("OriginCountry", StringType)
      .add("DestCountry", StringType)
      .add("DestRegion", StringType)
      .add("OriginAirportID", StringType)
      .add("DestCityName", StringType)
      .add("timestamp", TimestampType)
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "csv",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val filterExpr = And(
      EqualTo(UnresolvedAttribute("Cancelled"), Literal(false)),
      GreaterThan(UnresolvedAttribute("DistanceMiles"), Literal(0)))
    val filter = Filter(filterExpr, relation)
    val project = Project(
      Seq(
        UnresolvedAttribute("FlightNum"),
        UnresolvedAttribute("DistanceMiles"),
        UnresolvedAttribute("FlightTimeMin"),
        UnresolvedAttribute("OriginWeather"),
        UnresolvedAttribute("AvgTicketPrice"),
        UnresolvedAttribute("Carrier"),
        UnresolvedAttribute("FlightDelayType"),
        UnresolvedAttribute("timestamp")),
      filter)
    val weatherAlias = Alias(UnresolvedAttribute("OriginWeather"), "OriginWeather")()
    val groupByAttributes = Seq(weatherAlias)
    val aggregateExpressions =
      Alias(
        UnresolvedFunction(
          Seq("AVG"),
          Seq(UnresolvedAttribute("AvgTicketPrice")),
          isDistinct = false),
        "avg_price_by_weather")()
    val aggregatePlan =
      Aggregate(groupByAttributes, Seq(aggregateExpressions, weatherAlias), project)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregatePlan)
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("test parquet file source relation") {
    val frame = sql(s"""
         | file = $testTable ( "integ-test/src/integration/resources/users.parquet" )
         | | fields name, favorite_color
         | """.stripMargin)

    val results: Array[Row] = frame.collect()
    // results.foreach(println(_))
    val expectedResults: Array[Row] = Array(Row("Alyssa", null), Row("Ben", "red"))
    implicit val oneColRowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical

    val userSpecifiedSchema = new StructType()
      .add("name", StringType)
      .add("favorite_color", StringType)
      .add("favorite_numbers", new ArrayType(IntegerType, true))
    val dataSource =
      DataSource(
        spark,
        userSpecifiedSchema = Some(userSpecifiedSchema),
        className = "parquet",
        options = Map.empty)
    val relation = LogicalRelation(dataSource.resolveRelation(true), isStreaming = false)
    val expectedPlan =
      Project(Seq(UnresolvedAttribute("name"), UnresolvedAttribute("favorite_color")), relation)
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }
}
