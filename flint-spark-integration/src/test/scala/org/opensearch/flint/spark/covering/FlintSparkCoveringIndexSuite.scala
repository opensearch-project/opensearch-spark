/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class FlintSparkCoveringIndexSuite extends FlintSuite with Matchers {

  /** Test table name */
  val testTable = "spark_catalog.default.ci_test"

  test("get covering index name") {
    val index =
      FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test_ci_index"
  }

  test("get covering index name on table and index name with dots") {
    val testTableDots = "spark_catalog.default.test.2023.10"
    val index = new FlintSparkCoveringIndex("ci.01", testTableDots, Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test.2023.10_ci.01_index"
  }

  test("should fail if get index name without full table name") {
    val index = new FlintSparkCoveringIndex("ci", "test", Map("name" -> "string"))
    assertThrows[IllegalArgumentException] {
      index.name()
    }
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      FlintSparkCoveringIndex("ci", "default.test", Map.empty)
    }
  }

  test("should generate id column based on ID expression in index options") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index =
        FlintSparkCoveringIndex(
          "name_idx",
          testTable,
          Map("name" -> "string"),
          options = FlintSparkIndexOptions(Map("id_expression" -> "now()")))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .withColumn(ID_COLUMN, expr("now()"))
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  test("should generate id column based on timestamp column if found") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex("name_idx", testTable, Map("name" -> "string"))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .withColumn(ID_COLUMN, sha1(concat(input_file_name(), col("timestamp"))))
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  test("should generate id column based on @timestamp column if found") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (`@timestamp` TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex("name_idx", testTable, Map("name" -> "string"))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .withColumn(ID_COLUMN, sha1(concat(input_file_name(), col("@timestamp"))))
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  test("should not generate id column if no ID expression or timestamp column") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (name STRING, age INTEGER) USING JSON")
      val index = FlintSparkCoveringIndex("name_idx", testTable, Map("name" -> "string"))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .select(col("name")))
    }
  }

  test("should generate id column if micro batch has timestamp column") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex("name_idx", testTable, Map("name" -> "string"))
      val batch = spark.read.table(testTable).select("timestamp", "name")

      assertDataFrameEquals(
        index.build(spark, Some(batch)),
        batch
          .withColumn(ID_COLUMN, sha1(concat(input_file_name(), col("timestamp"))))
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  test("should not generate id column if micro batch doesn't have timestamp column") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex("name_idx", testTable, Map("name" -> "string"))
      val batch = spark.read.table(testTable).select("name")

      assertDataFrameEquals(index.build(spark, Some(batch)), batch.select(col("name")))
    }
  }

  test("should build with filtering condition") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex(
        "name_idx",
        testTable,
        Map("name" -> "string"),
        Some("name = 'test'"))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .withColumn(ID_COLUMN, sha1(concat(input_file_name(), col("timestamp"))))
          .where("name = 'test'")
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  /* Assert unresolved logical plan in DataFrame equals without semantic analysis */
  private def assertDataFrameEquals(df1: DataFrame, df2: DataFrame): Unit = {
    comparePlans(df1.queryExecution.logical, df2.queryExecution.logical, checkAnalysis = false)
  }
}
