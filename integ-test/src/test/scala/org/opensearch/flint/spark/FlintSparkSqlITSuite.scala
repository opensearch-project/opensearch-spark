/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty

import org.opensearch.flint.OpenSearchSuite
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf.{HOST_ENDPOINT, HOST_PORT, REFRESH_POLICY}
import org.apache.spark.sql.streaming.StreamTest

class FlintSparkSqlITSuite
    extends QueryTest
    with FlintSuite
    with OpenSearchSuite
    with StreamTest {

  /** Flint Spark high level API for assertion */
  private lazy val flint: FlintSpark = new FlintSpark(spark)

  /** Test table and index name */
  private val testTable = "default.flint_sql_test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Configure for FlintSpark explicit created above and the one behind Flint SQL
    setFlintSparkConf(HOST_ENDPOINT, openSearchHost)
    setFlintSparkConf(HOST_PORT, openSearchPort)
    setFlintSparkConf(REFRESH_POLICY, true)

    // Create test table
    sql(s"""
           | CREATE TABLE $testTable
           | (
           |   name STRING,
           |   age INT
           | )
           | USING CSV
           | OPTIONS (
           |  header 'false',
           |  delimiter '\t'
           | )
           | PARTITIONED BY (
           |    year INT,
           |    month INT
           | )
           |""".stripMargin)

    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 30)
           | """.stripMargin)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    flint.deleteIndex(testIndex)

    // Stop all streaming jobs if any
    spark.streams.active.foreach { job =>
      job.stop()
      job.awaitTermination()
    }
  }

  test("create skipping index with auto refresh") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (
           |   year PARTITION,
           |   name VALUE_SET,
           |   age MIN_MAX
           | )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testIndex)
    job shouldBe defined
    failAfter(streamingTimeout) {
      job.get.processAllAvailable()
    }

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)
    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 1
  }

  test("create skipping index with manual refresh") {
    sql(s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | """.stripMargin)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)

    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH SKIPPING INDEX ON $testTable")
    indexData.count() shouldBe 1
  }

  test("describe skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .addValueSet("name")
      .addMinMax("age")
      .create()

    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(
      result,
      Seq(
        Row("year", "int", "PARTITION"),
        Row("name", "string", "VALUE_SET"),
        Row("age", "int", "MIN_MAX")))
  }

  test("create skipping index on table without database name") {
    sql(s"""
           | CREATE SKIPPING INDEX ON flint_sql_test
           | (
           |   year PARTITION,
           |   name VALUE_SET,
           |   age MIN_MAX
           | )
           | """.stripMargin)

    flint.describeIndex(testIndex) shouldBe defined
  }

  test("create skipping index on table in other database") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")
    sql("CREATE TABLE test (name STRING) USING CSV")
    sql("CREATE SKIPPING INDEX ON test (name VALUE_SET)")

    flint.describeIndex("flint_sample_test_skipping_index") shouldBe defined
  }

  test("should return empty if no skipping index to describe") {
    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(result, Seq.empty)
  }

  test("drop skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()

    sql(s"DROP SKIPPING INDEX ON $testTable")

    flint.describeIndex(testIndex) shouldBe empty
  }
}
