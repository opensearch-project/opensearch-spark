/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import java.nio.file.{Files, Path}

import org.opensearch.sql.ppl.utils.DataTypeTransformer.seq

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Expression, Literal, SortOrder}
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
    //    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Double](_.getAs[Double](1))
    assert(results.sorted.sameElements(expectedResults.sorted))
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
//    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, Long](_.getAs[Long](3))
    assert(results.sorted.sameElements(expectedResults.sorted))
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
  }
}
