/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.json4s.native.JsonMethods.parse
import org.mockito.Mockito.when
import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.{FILE_PATH_COLUMN, SKIPPING_INDEX_TYPE}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.FlintSuite
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.functions.col

class FlintSparkSkippingIndexSuite extends FlintSuite {

  private val testTable = "spark_catalog.default.test"

  test("get skipping index name") {
    val index = new FlintSparkSkippingIndex(testTable, Seq(mock[FlintSparkSkippingStrategy]))
    index.name() shouldBe "flint_spark_catalog_default_test_skipping_index"
  }

  test("get skipping index name on table name with dots") {
    val testTableDots = "spark_catalog.default.test.2023.10"
    val index = new FlintSparkSkippingIndex(testTableDots, Seq(mock[FlintSparkSkippingStrategy]))
    index.name() shouldBe "flint_spark_catalog_default_test.2023.10_skipping_index"
  }

  test("get index metadata") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.columnName).thenReturn("test_field")
    when(indexCol.columnType).thenReturn("integer")
    when(indexCol.outputSchema()).thenReturn(Map("test_field" -> "integer"))
    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))

    val metadata = index.metadata()
    metadata.kind shouldBe SKIPPING_INDEX_TYPE
    metadata.name shouldBe index.name()
    metadata.source shouldBe testTable
    metadata.indexedColumns shouldBe Array(
      Map(
        "kind" -> SkippingKind.PARTITION.toString,
        "columnName" -> "test_field",
        "columnType" -> "integer").asJava)
  }

  test("can build index building job with unique ID column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("name" -> "string"))
    when(indexCol.getAggregators).thenReturn(
      Seq(CollectSet(col("name").expr).toAggregateExpression()))
    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(spark, Some(df))
    indexDf.schema.fieldNames should contain only ("name", FILE_PATH_COLUMN, ID_COLUMN)
  }

  test("can build index on table name with special characters") {
    val testTableSpecial = "spark_catalog.default.test/2023/10"
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("name" -> "string"))
    when(indexCol.getAggregators).thenReturn(
      Seq(CollectSet(col("name").expr).toAggregateExpression()))
    val index = new FlintSparkSkippingIndex(testTableSpecial, Seq(indexCol))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(spark, Some(df))
    indexDf.schema.fieldNames should contain only ("name", FILE_PATH_COLUMN, ID_COLUMN)
  }

  // Test index build for different column type
  Seq(
    (
      "boolean_col",
      "boolean",
      """{
        |  "boolean_col": {
        |    "type": "boolean"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "string_col",
      "string",
      """{
        |  "string_col": {
        |    "type": "keyword"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "varchar_col",
      "varchar(20)",
      """{
        |  "varchar_col": {
        |    "type": "keyword"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "char_col",
      "char(20)",
      """{
        |  "char_col": {
        |    "type": "keyword"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "long_col",
      "bigint",
      """{
        |  "long_col": {
        |    "type": "long"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "int_col",
      "int",
      """{
        |  "int_col": {
        |    "type": "integer"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "short_col",
      "smallint",
      """{
        |  "short_col": {
        |    "type": "short"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "byte_col",
      "tinyint",
      """{
        |  "byte_col": {
        |    "type": "byte"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "double_col",
      "double",
      """{
        |  "double_col": {
        |    "type": "double"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "float_col",
      "float",
      """{
        |  "float_col": {
        |    "type": "float"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "timestamp_col",
      "timestamp",
      """{
        |  "timestamp_col": {
        |    "type": "date",
        |    "format": "strict_date_optional_time_nanos"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "date_col",
      "date",
      """{
        |  "date_col": {
        |    "type": "date",
        |    "format": "strict_date"
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}"""),
    (
      "struct_col",
      "struct<subfield1:string,subfield2:int>",
      """{
        |  "struct_col": {
        |    "properties": {
        |      "subfield1": {
        |        "type": "keyword"
        |      },
        |      "subfield2": {
        |        "type": "integer"
        |      }
        |    }
        |  },
        |  "file_path": {
        |    "type": "keyword"
        |  }
        |}""")).foreach { case (columnName, columnType, expectedSchema) =>
    test(s"can build index for $columnType column") {
      val indexCol = mock[FlintSparkSkippingStrategy]
      when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
      when(indexCol.outputSchema()).thenReturn(Map(columnName -> columnType))
      when(indexCol.getAggregators).thenReturn(
        Seq(CollectSet(col(columnName).expr).toAggregateExpression()))

      val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
      schemaShouldMatch(index.metadata(), expectedSchema.stripMargin)
    }
  }

  test("should fail if get index name without full table name") {
    assertThrows[IllegalArgumentException] {
      FlintSparkSkippingIndex.getSkippingIndexName("test")
    }
  }

  test("should fail if no indexed column given") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkSkippingIndex(testTable, Seq.empty)
    }
  }

  private def schemaShouldMatch(metadata: FlintMetadata, expected: String): Unit = {
    val actual = parse(metadata.getContent) \ "properties"
    assert(actual == parse(expected))
  }
}
