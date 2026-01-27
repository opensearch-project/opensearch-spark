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
import org.apache.spark.sql.types._

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
        varchar_col VARCHAR(100),
        char_col CHAR(10),
        binary_col BINARY,
        date_col DATE,
        timestamp_col TIMESTAMP,
        timestamp_ntz_col TIMESTAMP_NTZ
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
        "REAL float_col, " +
        "DOUBLE double_col, " +
        "DECIMAL(10, 2) decimal_col, " +
        "VARCHAR string_col, " +
        "VARCHAR(100) varchar_col, " +
        "CHAR(10) char_col, " +
        "VARBINARY binary_col, " +
        "DATE date_col, " +
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) timestamp_col, " +
        "TIMESTAMP(0) timestamp_ntz_col)"
    }
  }

  test("interval types") {
    withMockTable(Some("""
        interval_day INTERVAL DAY,
        interval_day_hour INTERVAL DAY TO HOUR,
        interval_day_minute INTERVAL DAY TO MINUTE,
        interval_day_second INTERVAL DAY TO SECOND,
        interval_hour INTERVAL HOUR,
        interval_hour_minute INTERVAL HOUR TO MINUTE,
        interval_hour_second INTERVAL HOUR TO SECOND,
        interval_minute INTERVAL MINUTE,
        interval_minute_second INTERVAL MINUTE TO SECOND,
        interval_second INTERVAL SECOND,
        interval_year INTERVAL YEAR,
        interval_year_month INTERVAL YEAR TO MONTH,
        interval_month INTERVAL MONTH
      """)) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

      table.getRowType(typeFactory).toString shouldBe
        "RecordType(" +
        "INTERVAL_DAY(2, 6) interval_day, " +
        "INTERVAL_DAY_HOUR(2, 6) interval_day_hour, " +
        "INTERVAL_DAY_MINUTE(2, 6) interval_day_minute, " +
        "INTERVAL_DAY_SECOND(2, 6) interval_day_second, " +
        "INTERVAL_HOUR(2, 6) interval_hour, " +
        "INTERVAL_HOUR_MINUTE(2, 6) interval_hour_minute, " +
        "INTERVAL_HOUR_SECOND(2, 6) interval_hour_second, " +
        "INTERVAL_MINUTE(2, 6) interval_minute, " +
        "INTERVAL_MINUTE_SECOND(2, 6) interval_minute_second, " +
        "INTERVAL_SECOND(2, 6) interval_second, " +
        "INTERVAL_YEAR interval_year, " +
        "INTERVAL_YEAR_MONTH interval_year_month, " +
        "INTERVAL_MONTH interval_month)"
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

  test("user-defined type") {

    /** Custom type that wraps a Long as its SQL type. */
    class CustomType extends UserDefinedType[AnyRef] {
      override def sqlType: DataType = LongType
      override def serialize(obj: AnyRef): Any = obj
      override def deserialize(datum: Any): AnyRef = datum.asInstanceOf[AnyRef]
      override def userClass: Class[AnyRef] = classOf[AnyRef]
    }

    val spark = mock[SparkSession]
    val df = mock[DataFrame]
    val schema = new StructType().add("point_col", new CustomType)
    when(df.schema).thenReturn(schema)
    when(spark.table(s"$testCatalog.$testDb.$testTable")).thenReturn(df)

    val sparkSchema = new SparkSchema(spark, testCatalog)
    val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

    // PointUDT.sqlType is LongType, which maps to BIGINT
    table.getRowType(typeFactory).toString shouldBe "RecordType(BIGINT point_col)"
  }

  test("nullability for complex and nested complex types") {
    withMockTable(Some("""
        array_col ARRAY<INT> NOT NULL,
        map_col MAP<STRING, INT> NOT NULL,
        struct_col STRUCT<name: STRING, age: INT> NOT NULL,
        nested_array ARRAY<STRUCT<id: INT, values: ARRAY<STRING>>> NOT NULL,
        nested_struct STRUCT<info: STRUCT<name: STRING, tags: ARRAY<STRING>>> NOT NULL
      """)) { spark =>
      val sparkSchema = new SparkSchema(spark, testCatalog)
      val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

      table.getRowType(typeFactory).getFullTypeString shouldBe
        "RecordType(" +
        "INTEGER ARRAY NOT NULL array_col, " +
        "(VARCHAR NOT NULL, INTEGER) MAP NOT NULL map_col, " +
        "RecordType(VARCHAR name, INTEGER age) NOT NULL struct_col, " +
        "RecordType(INTEGER id, VARCHAR ARRAY values) ARRAY NOT NULL nested_array, " +
        "RecordType(RecordType(VARCHAR name, VARCHAR ARRAY tags) info) NOT NULL nested_struct) NOT NULL"
    }
  }

  test("unsupported type fallback") {
    val spark = mock[SparkSession]
    val df = mock[DataFrame]
    val schema = new StructType()
      .add("calendar_interval_col", CalendarIntervalType)
      .add("object_col", ObjectType(classOf[AnyRef]))
    when(df.schema).thenReturn(schema)
    when(spark.table(s"$testCatalog.$testDb.$testTable")).thenReturn(df)

    val sparkSchema = new SparkSchema(spark, testCatalog)
    val table = sparkSchema.subSchemas().get(testDb).tables().get(testTable)

    table.getRowType(typeFactory).toString shouldBe
      "RecordType(ANY calendar_interval_col, ANY object_col)"
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
