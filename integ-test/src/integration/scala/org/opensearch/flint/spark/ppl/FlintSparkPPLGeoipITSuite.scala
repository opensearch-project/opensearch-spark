/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.sql.{QueryTest, Row}
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

  test("test geoip with no parameters") {
    val frame = sql(s"""
         | source = $testTable| where isValid = true | eval a = geoip(ip) | fields ip, a
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
  }

  test("test geoip with one parameters") {
    val frame = sql(s"""
         | source = $testTable| where isValid = true | eval a = geoip(ip, country_name) | fields ip, a
         | """.stripMargin)

    // Retrieve the results
    val results: Array[Row] = frame.collect()
    // Define the expected results
    val expectedResults: Array[Row] =
      Array(Row("66.249.157.90", "Jamaica"), Row("2a09:bac2:19f8:2ac3::", "Canada"))

    // Compare the results
    implicit val rowOrdering: Ordering[Row] = Ordering.by[Row, String](_.getAs[String](0))
    assert(results.sorted.sameElements(expectedResults.sorted))
  }

  test("test geoip with multiple parameters") {
    val frame = sql(s"""
         | source = $testTable| where isValid = true | eval a = geoip(ip, country_name, city_name) | fields ip, a
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
  }
}
