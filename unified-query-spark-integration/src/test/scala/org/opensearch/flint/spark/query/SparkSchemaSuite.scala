/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import scala.collection.JavaConverters._

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
 * Test suite for SparkSchema that verifies Spark catalog to Calcite schema mapping. Uses mocked
 * SparkSession to avoid heavyweight Spark initialization.
 */
class SparkSchemaSuite extends SparkFunSuite with Matchers with MockitoSugar {

  private val typeFactory = new JavaTypeFactoryImpl()

  /**
   * Extracts field names and their SQL type names from a RelDataType for readable assertions.
   */
  private def fieldTypes(relType: RelDataType): Seq[(String, SqlTypeName)] =
    relType.getFieldList.asScala.toSeq.map(f => f.getName -> f.getType.getSqlTypeName)

  /**
   * Creates a mock SparkSession that returns a DataFrame with the given schema when spark.table()
   * is called with the specified table name.
   */
  private def mockSparkWithTable(
      catalogName: String,
      dbName: String,
      tableName: String,
      schema: StructType): SparkSession = {
    val spark = mock[SparkSession]
    val df = mock[DataFrame]
    when(df.schema).thenReturn(schema)
    when(spark.table(s"$catalogName.$dbName.$tableName")).thenReturn(df)
    spark
  }

  test("SparkSchema should resolve table with primitive types") {
    val schema = StructType(
      Seq(
        StructField("bool_col", BooleanType),
        StructField("byte_col", ByteType),
        StructField("short_col", ShortType),
        StructField("int_col", IntegerType),
        StructField("long_col", LongType),
        StructField("float_col", FloatType),
        StructField("double_col", DoubleType),
        StructField("decimal_col", DecimalType(10, 2)),
        StructField("string_col", StringType),
        StructField("binary_col", BinaryType),
        StructField("date_col", DateType),
        StructField("timestamp_col", TimestampType)))

    val spark = mockSparkWithTable("spark_catalog", "test_db", "primitive_types", schema)
    val sparkSchema = new SparkSchema(spark, "spark_catalog")
    val table = sparkSchema.getSubSchema("test_db").getTable("primitive_types")

    table should not be null

    val rowType = table.getRowType(typeFactory)

    // Verify all field types in one readable assertion
    fieldTypes(rowType) shouldBe Seq(
      "bool_col" -> SqlTypeName.BOOLEAN,
      "byte_col" -> SqlTypeName.TINYINT,
      "short_col" -> SqlTypeName.SMALLINT,
      "int_col" -> SqlTypeName.INTEGER,
      "long_col" -> SqlTypeName.BIGINT,
      "float_col" -> SqlTypeName.FLOAT,
      "double_col" -> SqlTypeName.DOUBLE,
      "decimal_col" -> SqlTypeName.DECIMAL,
      "string_col" -> SqlTypeName.VARCHAR,
      "binary_col" -> SqlTypeName.VARBINARY,
      "date_col" -> SqlTypeName.DATE,
      "timestamp_col" -> SqlTypeName.TIMESTAMP)

    // Verify decimal precision and scale
    val decimalType = rowType.getField("decimal_col", true, false).getType
    decimalType.getPrecision shouldBe 10
    decimalType.getScale shouldBe 2
  }

  test("SparkSchema should resolve table with complex types") {
    val schema = StructType(
      Seq(
        StructField("array_col", ArrayType(IntegerType)),
        StructField("map_col", MapType(StringType, IntegerType)),
        StructField(
          "struct_col",
          StructType(Seq(StructField("name", StringType), StructField("age", IntegerType))))))

    val spark = mockSparkWithTable("spark_catalog", "test_db", "complex_types", schema)
    val sparkSchema = new SparkSchema(spark, "spark_catalog")
    val table = sparkSchema.getSubSchema("test_db").getTable("complex_types")
    val rowType = table.getRowType(typeFactory)

    fieldTypes(rowType) shouldBe Seq(
      "array_col" -> SqlTypeName.ARRAY,
      "map_col" -> SqlTypeName.MAP,
      "struct_col" -> SqlTypeName.ROW)

    // Verify array element type
    val arrayType = rowType.getField("array_col", true, false).getType
    arrayType.getComponentType.getSqlTypeName shouldBe SqlTypeName.INTEGER

    // Verify map key/value types
    val mapType = rowType.getField("map_col", true, false).getType
    mapType.getKeyType.getSqlTypeName shouldBe SqlTypeName.VARCHAR
    mapType.getValueType.getSqlTypeName shouldBe SqlTypeName.INTEGER

    // Verify struct field types
    val structType = rowType.getField("struct_col", true, false).getType
    fieldTypes(structType) shouldBe Seq(
      "name" -> SqlTypeName.VARCHAR,
      "age" -> SqlTypeName.INTEGER)
  }

  test("SparkSchema should handle nested complex types") {
    val schema = StructType(
      Seq(
        StructField(
          "nested_array",
          ArrayType(StructType(
            Seq(StructField("id", IntegerType), StructField("values", ArrayType(StringType)))))),
        StructField(
          "nested_struct",
          StructType(
            Seq(
              StructField(
                "info",
                StructType(Seq(
                  StructField("name", StringType),
                  StructField("tags", ArrayType(StringType))))))))))

    val spark = mockSparkWithTable("spark_catalog", "test_db", "nested_types", schema)
    val sparkSchema = new SparkSchema(spark, "spark_catalog")
    val table = sparkSchema.getSubSchema("test_db").getTable("nested_types")
    val rowType = table.getRowType(typeFactory)

    fieldTypes(rowType) shouldBe Seq(
      "nested_array" -> SqlTypeName.ARRAY,
      "nested_struct" -> SqlTypeName.ROW)

    // Verify nested array of structs: Array<Struct<id: Int, values: Array<String>>>
    val nestedArrayType = rowType.getField("nested_array", true, false).getType
    val elementType = nestedArrayType.getComponentType
    elementType.getSqlTypeName shouldBe SqlTypeName.ROW
    fieldTypes(elementType) shouldBe Seq(
      "id" -> SqlTypeName.INTEGER,
      "values" -> SqlTypeName.ARRAY)

    // Verify nested struct: Struct<info: Struct<name: String, tags: Array<String>>>
    val nestedStructType = rowType.getField("nested_struct", true, false).getType
    fieldTypes(nestedStructType) shouldBe Seq("info" -> SqlTypeName.ROW)

    val infoType = nestedStructType.getField("info", true, false).getType
    fieldTypes(infoType) shouldBe Seq("name" -> SqlTypeName.VARCHAR, "tags" -> SqlTypeName.ARRAY)
  }
}
