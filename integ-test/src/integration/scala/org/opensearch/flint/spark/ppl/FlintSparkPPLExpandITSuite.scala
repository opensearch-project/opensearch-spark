/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import java.nio.file.Files
import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Explode, GeneratorOuter, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkPPLExpandITSuite
  extends QueryTest
    with LogicalPlanTestUtils
    with FlintPPLSuite
    with StreamTest {

  private val testTable = "flint_ppl_test"
  private val structNestedTable = "spark_catalog.default.flint_ppl_struct_nested_test"
  private val structTable = "spark_catalog.default.flint_ppl_struct_test"
  private val multiValueTable = "spark_catalog.default.flint_ppl_multi_value_test"
  private val tempFile = Files.createTempFile("jsonTestData", ".json")

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create test table
    createNestedJsonContentTable(tempFile, testTable)
    createStructNestedTable(structNestedTable)
    createStructTable(structTable)
    createMultiValueStructTable(multiValueTable)
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

  private def generator(flattenedColumn: String, parentPlan: LogicalPlan): LogicalPlan = {
    val outerGenerator = GeneratorOuter(Explode(UnresolvedAttribute(flattenedColumn)))
    val generate = Generate(outerGenerator, seq(), outer = true, None, seq(), parentPlan)
    val dropSourceColumn =
      DataFrameDropColumns(Seq(UnresolvedAttribute(flattenedColumn)), generate)
    dropSourceColumn
  }

  test("expand for array of structs") {
    val frame = sql(s"""
                       | source = $testTable
                       | | where country = 'England' or country = 'Poland'
                       | | expand bridges 
                       | | fields city, country, col
                       | """.stripMargin)

    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row( "London", "England", Seq(801, "Tower Bridge")),
        Row( "London", "England", Seq(928, "London Bridge")),
        Row("Warsaw", "Poland", null))
    // Compare the results
    assert(results.toSet == expectedResults.toSet)
    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("flint_ppl_test"))
    val filter = Filter(
      Or(
        EqualTo(UnresolvedAttribute("country"), Literal("England")),
        EqualTo(UnresolvedAttribute("country"), Literal("Poland"))),
      table)
    val project = Project(Seq(UnresolvedAttribute("city"), UnresolvedAttribute("country"), UnresolvedAttribute("col")), filter)
    val generatorExp = generator("bridges", project)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), generatorExp)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("flatten for arrays") {
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
    val flattenBridges = generator("bridges", projectCoor)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), flattenBridges)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("flatten for structs and arrays") {
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
    val flattenBridges = generator("bridges", table)
    val flattenCoor = generator("coor", flattenBridges)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), flattenCoor)
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
    val flattenBridges = generator("bridges", projectCountryBridges)
    val projectCountryLength =
      Project(Seq(UnresolvedAttribute("country"), UnresolvedAttribute("length")), flattenBridges)
    val average = Alias(
      UnresolvedFunction(
        seq("AVG"),
        seq(UnresolvedAttribute("length")),
        isDistinct = false,
        None,
        ignoreNulls = false),
      "avg")()
    val country = Alias(UnresolvedAttribute("country"), "country")()
    val grouping = Alias(UnresolvedAttribute("country"), "country")()
    val aggregate = Aggregate(Seq(grouping), Seq(average, country), projectCountryLength)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), aggregate)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("flatten struct table") {
    val frame = sql(s"""
                       | source = $structTable
                       | | flatten struct_col
                       | | flatten field1
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("int_col", "field2", "subfield")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(Row(30, 123, "value1"), Row(40, 456, "value2"), Row(50, 789, "value3"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_struct_test"))
    val flattenStructCol = generator("struct_col", table)
    val flattenField1 = generator("field1", flattenStructCol)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), flattenField1)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("flatten struct nested table") {
    val frame = sql(s"""
                       | source = $structNestedTable
                       | | flatten struct_col
                       | | flatten field1
                       | | flatten struct_col2
                       | | flatten field1
                       | """.stripMargin)

    assert(
      frame.columns.sameElements(Array("int_col", "field2", "subfield", "field2", "subfield")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(30, 123, "value1", 23, "valueA"),
        Row(40, 123, "value5", 33, "valueB"),
        Row(30, 823, "value4", 83, "valueC"),
        Row(40, 456, "value2", 46, "valueD"),
        Row(50, 789, "value3", 89, "valueE"))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table =
      UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_struct_nested_test"))
    val flattenStructCol = generator("struct_col", table)
    val flattenField1 = generator("field1", flattenStructCol)
    val flattenStructCol2 = generator("struct_col2", flattenField1)
    val flattenField1Again = generator("field1", flattenStructCol2)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), flattenField1Again)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }

  test("flatten multi value nullable") {
    val frame = sql(s"""
                       | source = $multiValueTable
                       | | flatten multi_value
                       | """.stripMargin)

    assert(frame.columns.sameElements(Array("int_col", "name", "value")))
    val results: Array[Row] = frame.collect()
    val expectedResults: Array[Row] =
      Array(
        Row(1, "1_one", 1),
        Row(1, null, 11),
        Row(1, "1_three", null),
        Row(2, "2_Monday", 2),
        Row(2, null, null),
        Row(3, "3_third", 3),
        Row(3, "3_4th", 4),
        Row(4, null, null))
    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Int](_.getAs[Int](0))
    assert(results.sorted.sameElements(expectedResults.sorted))

    val logicalPlan: LogicalPlan = frame.queryExecution.logical
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_multi_value_test"))
    val flattenMultiValue = generator("multi_value", table)
    val expectedPlan = Project(Seq(UnresolvedStar(None)), flattenMultiValue)
    comparePlans(logicalPlan, expectedPlan, checkAnalysis = false)
  }
}
