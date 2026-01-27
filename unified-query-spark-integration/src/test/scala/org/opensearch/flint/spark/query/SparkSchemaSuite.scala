/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Test suite for SparkSchema that verifies Spark catalog to Calcite schema mapping.
 */
class SparkSchemaSuite extends SparkFunSuite with Matchers with MockitoSugar {

  private val typeFactory = new JavaTypeFactoryImpl()
  private val testCatalog = "catalog"
  private val testDb = "db"
  private val testTable = "table"

  test("getSubSchema for any database") {
    withMockTable(None) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val subSchema = sparkSchema.subSchemas().get("any_db")

      subSchema should not be null
    }
  }

  test("getTable for existing table") {
    withMockTable(Some("id INT")) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val subSchema = sparkSchema.subSchemas().get(testDb)
      val table = subSchema.tables().get(testTable)

      table should not be null
      table.getRowType(typeFactory) should not be null
    }
  }

  test("getTable for non-existent table") {
    withMockTable(None) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val subSchema = sparkSchema.subSchemas().get(testDb)

      the[RuntimeException] thrownBy {
        subSchema.tables().get("non_existent")
      }
    }
  }

  test("primitive types") {
    withMockTable(Some("""
        bool_col BOOLEAN,
        byte_col BYTE,
        short_col SHORT,
        int_col INT,
        long_col BIGINT,
        float_col FLOAT,
        double_col DOUBLE,
        decimal_col DECIMAL(10, 2),
        string_col STRING,
        binary_col BINARY,
        date_col DATE,
        timestamp_col TIMESTAMP
      """)) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

      table.getRowType(typeFactory).toString shouldBe
        "RecordType(" +
        "BOOLEAN bool_col, " +
        "TINYINT byte_col, " +
        "SMALLINT short_col, " +
        "INTEGER int_col, " +
        "BIGINT long_col, " +
        "FLOAT float_col, " +
        "DOUBLE double_col, " +
        "DECIMAL(10, 2) decimal_col, " +
        "VARCHAR string_col, " +
        "VARBINARY binary_col, " +
        "DATE date_col, " +
        "TIMESTAMP(0) timestamp_col)"
    }
  }

  test("complex types") {
    withMockTable(Some("""
        array_col ARRAY<INT>,
        map_col MAP<STRING, INT>,
        struct_col STRUCT<name: STRING, age: INT>
      """)) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

      table.getRowType(typeFactory).toString shouldBe
        "RecordType(" +
        "INTEGER ARRAY array_col, " +
        "(VARCHAR, INTEGER) MAP map_col, " +
        "RecordType(VARCHAR name, INTEGER age) struct_col)"
    }
  }

  test("nested complex types") {
    withMockTable(Some("""
        nested_array ARRAY<STRUCT<id: INT, values: ARRAY<STRING>>>,
        nested_struct STRUCT<info: STRUCT<name: STRING, tags: ARRAY<STRING>>>
      """)) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

      table.getRowType(typeFactory).toString shouldBe
        "RecordType(" +
        "RecordType(INTEGER id, VARCHAR ARRAY values) ARRAY nested_array, " +
        "RecordType(RecordType(VARCHAR name, VARCHAR ARRAY tags) info) nested_struct)"
    }
  }

  private def withMockTable(ddl: Option[String])(f: SparkSession => Unit): Unit = {
    val spark = mock[SparkSession]
    ddl match {
      case Some(schema) =>
        val table = mock[DataFrame]
        when(table.schema).thenReturn(StructType.fromDDL(schema))
        when(spark.table(s"$testCatalog.$testDb.$testTable")).thenReturn(table)
      case None =>
        when(spark.table(any[String])).thenThrow(new RuntimeException("Table not found"))
    }
    f(spark)
  }
}
