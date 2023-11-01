/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.covering

import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, input_file_name, sha1}

class FlintSparkCoveringIndexSuite extends FlintSuite with Matchers {

  test("get covering index name") {
    val index =
      FlintSparkCoveringIndex("ci", "spark_catalog.default.test", Map("name" -> "string"))
    index.name() shouldBe "flint_spark_catalog_default_test_ci_index"
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

  test("should generate id column based on timestamp column") {
    val testTable = "spark_catalog.default.ci_test"
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

  private def assertDataFrameEquals(df1: DataFrame, df2: DataFrame): Unit = {
    comparePlans(df1.queryExecution.logical, df2.queryExecution.logical, checkAnalysis = false)
  }
}
