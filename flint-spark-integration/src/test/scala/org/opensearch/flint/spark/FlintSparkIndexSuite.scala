/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndex.{addIdColumn, ID_COLUMN}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.FlintSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Add, ConcatWs, Literal, Sha1, StructsToJson}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class FlintSparkIndexSuite extends QueryTest with FlintSuite with Matchers {

  test("should add ID column if ID expression is provided") {
    val df = spark.createDataFrame(Seq((1, "Alice"), (2, "Bob"))).toDF("id", "name")
    val options = new FlintSparkIndexOptions(Map("id_expression" -> "id + 10"))

    val resultDf = addIdColumn(df, options)
    resultDf.idColumn() shouldBe Some(Add(UnresolvedAttribute("id"), Literal(10)))
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
    val options = FlintSparkIndexOptions(Map("id_expression" ->
      "sha1(concat_ws('\0',boolean_col,string_col,long_col,int_col,double_col,float_col,timestamp_col,date_col,to_json(struct_col),struct_col.subfield2))"))

    val resultDf = addIdColumn(aggregatedDf, options)
    resultDf.idColumn() shouldBe Some(
      UnresolvedFunction(
        "sha1",
        Seq(UnresolvedFunction(
          "concat_ws",
          Seq(
            Literal(UTF8String.fromString("\0"), StringType),
            UnresolvedAttribute(Seq("boolean_col")),
            UnresolvedAttribute(Seq("string_col")),
            UnresolvedAttribute(Seq("long_col")),
            UnresolvedAttribute(Seq("int_col")),
            UnresolvedAttribute(Seq("double_col")),
            UnresolvedAttribute(Seq("float_col")),
            UnresolvedAttribute(Seq("timestamp_col")),
            UnresolvedAttribute(Seq("date_col")),
            UnresolvedFunction(
              "to_json",
              Seq(UnresolvedAttribute(Seq("struct_col"))),
              isDistinct = false),
            UnresolvedAttribute(Seq("struct_col", "subfield2"))),
          isDistinct = false)),
        isDistinct = false))
    resultDf.select(ID_COLUMN).distinct().count() shouldBe 1
  }
}
