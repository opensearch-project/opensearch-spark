/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndex.{addIdColumn, ID_COLUMN}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.types.StructType

class FlintSparkIndexSuite extends QueryTest with FlintSuite with Matchers {

  test("should add ID column if ID expression is provided") {
    val df = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"))).toDF("id", "name")
    val options = new FlintSparkIndexOptions(Map("id_expression" -> "id + 10"))

    val resultDf = addIdColumn(df, options)
    checkAnswer(resultDf.select(ID_COLUMN), Seq(Row(11), Row(12)))
  }

  test("should not add ID column if ID expression is not provided") {
    val df = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"))).toDF("id", "name")
    val options = FlintSparkIndexOptions.empty

    val resultDf = addIdColumn(df, options)
    resultDf.columns should not contain ID_COLUMN
  }

  test("should not add ID column if ID expression is empty") {
    val df = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"))).toDF("id", "name")
    val options = FlintSparkIndexOptions.empty

    val resultDf = addIdColumn(df, options)
    resultDf.columns should not contain ID_COLUMN
  }

  test("should add ID column for aggregated query") {
    val df = spark
      .createDataFrame(Seq((1, "Alice"), (2, "Bob"), (3, "Alice")))
      .toDF("id", "name")
      .groupBy("name")
      .count()
    val options = FlintSparkIndexOptions.empty

    val resultDf = addIdColumn(df, options)
    resultDf.columns should contain(ID_COLUMN)
    resultDf.select(ID_COLUMN).distinct().count() shouldBe 2
  }

  test("should generate ID column for various column types") {
    val schema = StructType.fromDDL("""
      boolean_col BOOLEAN,
      string_col STRING,
      long_col LONG,
      int_col INT,
      double_col DOUBLE,
      float_col FLOAT,
      timestamp_col TIMESTAMP,
      date_col DATE,
      struct_col STRUCT<subfield1: STRING, subfield2: INT>
    """)
    val data = Seq(
      Row(
        true,
        "Alice",
        100L,
        10,
        10.5,
        3.14f,
        java.sql.Timestamp.valueOf("2024-01-01 10:00:00"),
        java.sql.Date.valueOf("2024-01-01"),
        Row("sub1", 1)))

    val aggregatedDf = spark
      .createDataFrame(sparkContext.parallelize(data), schema)
      .groupBy(
        "boolean_col",
        "string_col",
        "long_col",
        "int_col",
        "double_col",
        "float_col",
        "timestamp_col",
        "date_col",
        "struct_col",
        "struct_col.subfield2")
      .count()
    val options = FlintSparkIndexOptions.empty

    val resultDf = addIdColumn(aggregatedDf, options)
    resultDf.columns should contain(ID_COLUMN)
    resultDf.select(ID_COLUMN).distinct().count() shouldBe 1
  }
}
