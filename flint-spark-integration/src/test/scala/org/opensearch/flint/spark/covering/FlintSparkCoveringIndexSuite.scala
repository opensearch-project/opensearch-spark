/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.FlintSparkIndexOptions
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.Project

class FlintSparkCoveringIndexSuite extends FlintSuite {

  private val testTable = "spark_catalog.default.ci_test"

  test("get covering index name") {
    val index =
      new FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))
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

  test("can build index building job with unique ID column") {
    val index =
      new FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(spark, Some(df))
    indexDf.schema.fieldNames should contain only ("name")
  }

  test("can build index on table name with special characters") {
    val testTableSpecial = "spark_catalog.default.test/2023/10"
    val index = new FlintSparkCoveringIndex("ci", testTableSpecial, Map("name" -> "string"))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(spark, Some(df))
    indexDf.schema.fieldNames should contain only ("name")
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkCoveringIndex("ci", "default.test", Map.empty)
    }
  }

  test("build batch with ID expression option") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (timestamp TIMESTAMP, name STRING) USING JSON")
      val index =
        FlintSparkCoveringIndex(
          "name_idx",
          testTable,
          Map("name" -> "string"),
          options = FlintSparkIndexOptions(Map("id_expression" -> "name")))

      val batchDf = index.build(spark, None)
      batchDf.idColumn() shouldBe Some(UnresolvedAttribute(Seq("name")))
    }
  }

  test("build stream with ID expression option") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (name STRING, age INTEGER) USING JSON")
      val index = FlintSparkCoveringIndex(
        "name_idx",
        testTable,
        Map("name" -> "string"),
        options =
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "id_expression" -> "name")))

      val streamDf = index.build(spark, Some(spark.table(testTable)))
      streamDf.idColumn() shouldBe Some(UnresolvedAttribute(Seq("name")))
    }
  }
}
