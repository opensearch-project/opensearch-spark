/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import java.nio.file.Files

import org.opensearch.flint.spark.FlattenGenerator
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, GeneratorOuter, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLFlattenITSuite
    extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "flint_ppl_test"
  private val tempFile = Files.createTempFile("jsonTestData", ".json")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createSimpleNestedJsonContentTable(tempFile, testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Files.deleteIfExists(tempFile)
  }

  test("test flatten for structs") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where country = 'England' or country = 'Poland'
                       | | fields coor
                       | | flatten coor
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("alt", "lat", "long")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(35, 51.5074, -0.1278), Row(null, null, null))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](1))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val filter = Filter(
      Or(
        EqualTo(UnresolvedAttribute("country"), Literal("England")),
        EqualTo(UnresolvedAttribute("country"), Literal("Poland"))),
      table)
    val projectCoor = Project(Seq(UnresolvedAttribute("coor")), filter)
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("coor"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate = Generate(outerGenerator, seq(), true, None, seq(), projectCoor)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("coor")), generate)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test flatten for arrays") {
    val frame = sql(s"""
                       | source = $testTable
                       | | fields bridges
                       | | flatten bridges
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("length", "name")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(null, null),
        Row(11L, "Bridge of Sighs"),
        Row(48L, "Rialto Bridge"),
        Row(160L, "Pont Alexandre III"),
        Row(232L, "Pont Neuf"),
        Row(801L, "Tower Bridge"),
        Row(928L, "London Bridge"),
        Row(343L, "Legion Bridge"),
        Row(516L, "Charles Bridge"),
        Row(333L, "Liberty Bridge"),
        Row(375L, "Chain Bridge"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val projectCoor = Project(Seq(UnresolvedAttribute("bridges")), table)
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("bridges"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate = Generate(outerGenerator, seq(), true, None, seq(), projectCoor)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute("bridges")), generate)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropSourceColumn)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test flatten for structs and arrays") {
    val frame = sql(s"""
                       | source = $testTable  | flatten bridges | flatten coor
                       | """.stripMargin)

    assert(
      frame.columns.sameElements(
        Array("_time", "city", "country", "length", "name", "alt", "lat", "long")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row("1990-09-13T12:00:00", "Warsaw", "Poland", null, null, null, null, null),
        Row(
          "2024-09-13T12:00:00",
          "Venice",
          "Italy",
          11L,
          "Bridge of Sighs",
          2,
          45.4408,
          12.3155),
        Row("2024-09-13T12:00:00", "Venice", "Italy", 48L, "Rialto Bridge", 2, 45.4408, 12.3155),
        Row(
          "2024-09-13T12:00:00",
          "Paris",
          "France",
          160L,
          "Pont Alexandre III",
          35,
          48.8566,
          2.3522),
        Row("2024-09-13T12:00:00", "Paris", "France", 232L, "Pont Neuf", 35, 48.8566, 2.3522),
        Row(
          "2024-09-13T12:00:00",
          "London",
          "England",
          801L,
          "Tower Bridge",
          35,
          51.5074,
          -0.1278),
        Row(
          "2024-09-13T12:00:00",
          "London",
          "England",
          928L,
          "London Bridge",
          35,
          51.5074,
          -0.1278),
        Row(
          "2024-09-13T12:00:00",
          "Prague",
          "Czech Republic",
          343L,
          "Legion Bridge",
          200,
          50.0755,
          14.4378),
        Row(
          "2024-09-13T12:00:00",
          "Prague",
          "Czech Republic",
          516L,
          "Charles Bridge",
          200,
          50.0755,
          14.4378),
        Row(
          "2024-09-13T12:00:00",
          "Budapest",
          "Hungary",
          333L,
          "Liberty Bridge",
          96,
          47.4979,
          19.0402),
        Row(
          "2024-09-13T12:00:00",
          "Budapest",
          "Hungary",
          375L,
          "Chain Bridge",
          96,
          47.4979,
          19.0402))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](3))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val flattenGeneratorBridges = new FlattenGenerator(UnresolvedAttribute("bridges"))
    val outerGeneratorBridges = GeneratorOuter(flattenGeneratorBridges)
    val generateBridges = Generate(outerGeneratorBridges, seq(), true, None, seq(), table)
    val dropSourceColumnBridges =
      DataFrameDropColumns(Seq(UnresolvedAttribute("bridges")), generateBridges)
    val flattenGeneratorCoor = new FlattenGenerator(UnresolvedAttribute("coor"))
    val outerGeneratorCoor = GeneratorOuter(flattenGeneratorCoor)
    val generateCoor =
      Generate(outerGeneratorCoor, seq(), true, None, seq(), dropSourceColumnBridges)
    val dropSourceColumnCoor =
      DataFrameDropColumns(Seq(UnresolvedAttribute("coor")), generateCoor)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), dropSourceColumnCoor)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("test flatten and stats") {
    val frame = sql(s"""
                       | source = $testTable
                       | | fields country, bridges
                       | | flatten bridges
                       | | fields country, length
                       | | stats avg(length) as avg by country
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(null, "Poland"),
        Row(196d, "France"),
        Row(429.5, "Czech Republic"),
        Row(864.5, "England"),
        Row(29.5, "Italy"),
        Row(354.0, "Hungary"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val projectCountryBridges =
      Project(Seq(UnresolvedAttribute("country"), UnresolvedAttribute("bridges")), table)
    val flattenGenerator = new FlattenGenerator(UnresolvedAttribute("bridges"))
    val outerGenerator = GeneratorOuter(flattenGenerator)
    val generate = Generate(outerGenerator, seq(), true, None, seq(), projectCountryBridges)
    val dropSourceColumn = DataFrameDropColumns(Seq(UnresolvedAttribute("bridges")), generate)
    val projectCountryLength = Project(
      Seq(UnresolvedAttribute("country"), UnresolvedAttribute("length")),
      dropSourceColumn)
    val average = Alias(
      UnresolvedFunction(seq("AVG"), seq(UnresolvedAttribute("length")), false, None, false),
      "avg")()
    val country = Alias(UnresolvedAttribute("country"), "country")()
    val grouping = Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(average, country), projectCountryLength)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
