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
import org.apache.spark.sql.flint.config.FlintSparkConf.OPTIMIZER_RULE_ENABLED
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
          options = FlintSparkIndexOptions(Map("id_expression" -> "timestamp")))

      assertDataFrameEquals(
        index.build(spark, None),
        spark
          .table(testTable)
          .withColumn(ID_COLUMN, expr("timestamp"))
          .select(col("name"), col(ID_COLUMN)))
    }
  }

  test("should build without ID column if not auto refreshed") {
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

  test(
    "should build failed if auto refresh and checkpoint location provided but no ID generated") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (name STRING, age INTEGER) USING JSON")
      val index = FlintSparkCoveringIndex(
        "name_idx",
        testTable,
        Map("name" -> "string"),
        options = FlintSparkIndexOptions(
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3://test/")))

      assertThrows[IllegalStateException] {
        index.build(spark, None)
      }
    }
  }

  test(
    "should build failed if auto refresh and checkpoint location provided but micro batch doesn't have ID generated") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index = FlintSparkCoveringIndex(
        "name_idx",
        testTable,
        Map("name" -> "string"),
        options = FlintSparkIndexOptions(
          Map("auto_refresh" -> "true", "checkpoint_location" -> "s3://test/")))
      val batch = spark.read.table(testTable).select("name")

      assertThrows[IllegalStateException] {
        index.build(spark, Some(batch))
      }
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

      // Avoid optimizer rule to check Flint index exists
      spark.conf.set(OPTIMIZER_RULE_ENABLED.key, "false")
      try {
        assertDataFrameEquals(
          index.build(spark, None),
          spark
            .table(testTable)
            .where("name = 'test'")
            .select(col("name")))
      } finally {
        spark.conf.set(OPTIMIZER_RULE_ENABLED.key, "true")
      }
    }
  }

  /* Assert unresolved logical plan in DataFrame equals without semantic analysis */
  private def assertDataFrameEquals(df1: DataFrame, df2: DataFrame): Unit = {
    comparePlans(df1.queryExecution.logical, df2.queryExecution.logical, checkAnalysis = false)
  }
}
