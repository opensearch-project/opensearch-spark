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

  test("get index metadata") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.columnName).thenReturn("test_field")
    when(indexCol.columnType).thenReturn("integer")
    when(indexCol.outputSchema()).thenReturn(Map("test_field" -> "integer"))
    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))

    val metadata = index.metadata()
    metadata.kind shouldBe SKIPPING_INDEX_TYPE
    metadata.name shouldBe ""
    metadata.source shouldBe testTable
    metadata.indexedColumns shouldBe Array(
      Map(
        "kind" -> SkippingKind.PARTITION.toString,
        "columnName" -> "test_field",
        "columnType" -> "integer").asJava)
  }

  test("should succeed if filtering condition is conjunction") {
    new FlintSparkSkippingIndex(
      testTable,
      Seq(mock[FlintSparkSkippingStrategy]),
      Some("test_field1 = 1 AND test_field2 = 2"))
  }

  test("should fail if filtering condition is not conjunction") {
    assertThrows[IllegalArgumentException] {
      new FlintSparkSkippingIndex(
        testTable,
        Seq(mock[FlintSparkSkippingStrategy]),
        Some("test_field1 = 1 OR test_field2 = 2"))
    }
  }

  test("can build index building job with unique ID column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.outputSchema()).thenReturn(Map("name" -> "string"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("name").expr)))
    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))

    val df = spark.createDataFrame(Seq(("hello", 20))).toDF("name", "age")
    val indexDf = index.build(spark, Some(df))
    indexDf.schema.fieldNames should contain only ("name", FILE_PATH_COLUMN, ID_COLUMN)
  }

  test("can build index for boolean column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("boolean_col" -> "boolean"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("boolean_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "boolean_col": {
         |    "type": "boolean"
         |  },
         |  "file_path": {
         |     "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for string column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("string_col" -> "string"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("string_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "string_col": {
         |    "type": "keyword"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  // TODO: test for osType "text"

  test("can build index for varchar column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("varchar_col" -> "varchar(20)"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("varchar_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "varchar_col": {
         |    "type": "keyword"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for char column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("char_col" -> "char(20)"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("char_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "char_col": {
         |    "type": "keyword"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for long column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("long_col" -> "bigint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("long_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "long_col": {
         |    "type": "long"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for int column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("int_col" -> "int"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("int_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "int_col": {
         |    "type": "integer"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for short column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("short_col" -> "smallint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("short_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "short_col": {
         |    "type": "short"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for byte column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("byte_col" -> "tinyint"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("byte_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "byte_col": {
         |    "type": "byte"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for double column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("double_col" -> "double"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("double_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "double_col": {
         |    "type": "double"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for float column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("float_col" -> "float"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("float_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "float_col": {
         |    "type": "float"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for timestamp column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("timestamp_col" -> "timestamp"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("timestamp_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "timestamp_col": {
         |    "type": "date",
         |    "format": "strict_date_optional_time_nanos"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for date column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema()).thenReturn(Map("date_col" -> "date"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("date_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
         |  "date_col": {
         |    "type": "date",
         |    "format": "strict_date"
         |  },
         |  "file_path": {
         |    "type": "keyword"
         |  }
         |}
         |""".stripMargin)
  }

  test("can build index for struct column") {
    val indexCol = mock[FlintSparkSkippingStrategy]
    when(indexCol.kind).thenReturn(SkippingKind.PARTITION)
    when(indexCol.outputSchema())
      .thenReturn(Map("struct_col" -> "struct<subfield1:string,subfield2:int>"))
    when(indexCol.getAggregators).thenReturn(Seq(CollectSet(col("struct_col").expr)))

    val index = new FlintSparkSkippingIndex(testTable, Seq(indexCol))
    schemaShouldMatch(
      index.metadata(),
      s"""{
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
         |}
         |""".stripMargin)
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
