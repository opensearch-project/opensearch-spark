/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.query

import scala.collection.JavaConverters._

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql.`type`.SqlTypeName

import org.apache.spark.sql.types._

/**
 * Bidirectional type conversion between Calcite and Spark type systems.
 *
 * This utility handles the mapping between Spark DataTypes and Calcite RelDataTypes, supporting
 * primitive types, complex types (arrays, maps, structs), and User Defined Types (UDTs) like
 * IPAddress and GeoPoint.
 */
object SparkCalciteTypeConverter {

  /**
   * Converts a Spark StructType to a Calcite row type (RelDataType).
   *
   * @param structType
   *   Spark StructType to convert
   * @param typeFactory
   *   Calcite type factory for creating types
   * @return
   *   Calcite RelDataType representing the struct
   */
  def toCalciteRowType(structType: StructType, typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldNames = structType.fields.map(_.name).toList.asJava
    val fieldTypes =
      structType.fields.map(f => toCalciteType(f.dataType, typeFactory)).toList.asJava
    typeFactory.createStructType(fieldTypes, fieldNames)
  }

  /**
   * Converts a Spark DataType to a Calcite RelDataType.
   *
   * @param sparkType
   *   Spark DataType to convert
   * @param typeFactory
   *   Calcite type factory for creating types
   * @return
   *   Calcite RelDataType
   */
  def toCalciteType(sparkType: DataType, typeFactory: RelDataTypeFactory): RelDataType = {
    sparkType match {
      // Boolean type
      case BooleanType =>
        typeFactory.createSqlType(SqlTypeName.BOOLEAN)

      // Numeric types
      case ByteType =>
        typeFactory.createSqlType(SqlTypeName.TINYINT)
      case ShortType =>
        typeFactory.createSqlType(SqlTypeName.SMALLINT)
      case IntegerType =>
        typeFactory.createSqlType(SqlTypeName.INTEGER)
      case LongType =>
        typeFactory.createSqlType(SqlTypeName.BIGINT)
      case FloatType =>
        typeFactory.createSqlType(SqlTypeName.FLOAT)
      case DoubleType =>
        typeFactory.createSqlType(SqlTypeName.DOUBLE)
      case dt: DecimalType =>
        typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)

      // String types
      case StringType =>
        typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case _: VarcharType =>
        typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case _: CharType =>
        typeFactory.createSqlType(SqlTypeName.CHAR)

      // Binary type
      case BinaryType =>
        typeFactory.createSqlType(SqlTypeName.VARBINARY)

      // Date/Time types
      case DateType =>
        typeFactory.createSqlType(SqlTypeName.DATE)
      case TimestampType =>
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
      case TimestampNTZType =>
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP)

      // Complex types
      case ArrayType(elementType, _) =>
        val elementCalciteType = toCalciteType(elementType, typeFactory)
        typeFactory.createArrayType(elementCalciteType, -1)

      case MapType(keyType, valueType, _) =>
        val keyCalciteType = toCalciteType(keyType, typeFactory)
        val valueCalciteType = toCalciteType(valueType, typeFactory)
        typeFactory.createMapType(keyCalciteType, valueCalciteType)

      case structType: StructType =>
        toCalciteRowType(structType, typeFactory)

      // User Defined Types (UDTs)
      // IPAddressUDT and GeoPointUDT are represented as their underlying SQL types
      case udt: UserDefinedType[_] =>
        udt.typeName match {
          case "ip" =>
            // IPAddress is stored as a string
            typeFactory.createSqlType(SqlTypeName.VARCHAR)
          case "geo_point" =>
            // GeoPoint is stored as an array of doubles [lat, lon]
            val doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE)
            typeFactory.createArrayType(doubleType, 2)
          case _ =>
            // For other UDTs, convert the underlying SQL type
            toCalciteType(udt.sqlType, typeFactory)
        }

      // Null type
      case NullType =>
        typeFactory.createSqlType(SqlTypeName.NULL)

      // Calendar interval (not directly supported in Calcite, use VARCHAR)
      case CalendarIntervalType =>
        typeFactory.createSqlType(SqlTypeName.VARCHAR)

      // Day-time interval
      case _: DayTimeIntervalType =>
        typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND)

      // Year-month interval
      case _: YearMonthIntervalType =>
        typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH)

      // Default fallback - treat as ANY type
      case _ =>
        typeFactory.createSqlType(SqlTypeName.ANY)
    }
  }

  /**
   * Converts a Calcite RelDataType to a Spark DataType.
   *
   * @param calciteType
   *   Calcite RelDataType to convert
   * @return
   *   Spark DataType
   */
  def toSparkType(calciteType: RelDataType): DataType = {
    calciteType.getSqlTypeName match {
      // Boolean type
      case SqlTypeName.BOOLEAN =>
        BooleanType

      // Numeric types
      case SqlTypeName.TINYINT =>
        ByteType
      case SqlTypeName.SMALLINT =>
        ShortType
      case SqlTypeName.INTEGER =>
        IntegerType
      case SqlTypeName.BIGINT =>
        LongType
      case SqlTypeName.FLOAT | SqlTypeName.REAL =>
        FloatType
      case SqlTypeName.DOUBLE =>
        DoubleType
      case SqlTypeName.DECIMAL =>
        val precision = calciteType.getPrecision
        val scale = calciteType.getScale
        DecimalType(precision, scale)

      // String types
      case SqlTypeName.CHAR =>
        StringType
      case SqlTypeName.VARCHAR =>
        StringType

      // Binary types
      case SqlTypeName.BINARY | SqlTypeName.VARBINARY =>
        BinaryType

      // Date/Time types
      case SqlTypeName.DATE =>
        DateType
      case SqlTypeName.TIME | SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE =>
        // Spark doesn't have a native TIME type, use TimestampType
        TimestampType
      case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        TimestampType

      // Interval types
      case SqlTypeName.INTERVAL_YEAR | SqlTypeName.INTERVAL_YEAR_MONTH |
          SqlTypeName.INTERVAL_MONTH =>
        YearMonthIntervalType()
      case SqlTypeName.INTERVAL_DAY | SqlTypeName.INTERVAL_DAY_HOUR |
          SqlTypeName.INTERVAL_DAY_MINUTE | SqlTypeName.INTERVAL_DAY_SECOND |
          SqlTypeName.INTERVAL_HOUR | SqlTypeName.INTERVAL_HOUR_MINUTE |
          SqlTypeName.INTERVAL_HOUR_SECOND | SqlTypeName.INTERVAL_MINUTE |
          SqlTypeName.INTERVAL_MINUTE_SECOND | SqlTypeName.INTERVAL_SECOND =>
        DayTimeIntervalType()

      // Complex types
      case SqlTypeName.ARRAY =>
        val componentType = calciteType.getComponentType
        if (componentType != null) {
          ArrayType(toSparkType(componentType), containsNull = true)
        } else {
          ArrayType(StringType, containsNull = true)
        }

      case SqlTypeName.MAP =>
        val keyType = calciteType.getKeyType
        val valueType = calciteType.getValueType
        if (keyType != null && valueType != null) {
          MapType(toSparkType(keyType), toSparkType(valueType), valueContainsNull = true)
        } else {
          MapType(StringType, StringType, valueContainsNull = true)
        }

      case SqlTypeName.ROW =>
        val fields = calciteType.getFieldList.asScala.map { field =>
          StructField(field.getName, toSparkType(field.getType), nullable = true)
        }
        StructType(fields.toArray)

      // Null type
      case SqlTypeName.NULL =>
        NullType

      // Any/Unknown type
      case SqlTypeName.ANY | SqlTypeName.OTHER =>
        StringType

      // Default fallback
      case _ =>
        StringType
    }
  }

  /**
   * Converts a Calcite row type (struct) to a Spark StructType.
   *
   * @param rowType
   *   Calcite row type
   * @return
   *   Spark StructType
   */
  def toSparkStructType(rowType: RelDataType): StructType = {
    val fields = rowType.getFieldList.asScala.map { field =>
      StructField(
        name = field.getName,
        dataType = toSparkType(field.getType),
        nullable = field.getType.isNullable)
    }
    StructType(fields.toArray)
  }

  /**
   * Creates a nullable version of the given Calcite type.
   *
   * @param calciteType
   *   Original Calcite type
   * @param typeFactory
   *   Type factory for creating the nullable type
   * @return
   *   Nullable version of the type
   */
  def makeNullable(calciteType: RelDataType, typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createTypeWithNullability(calciteType, true)
  }

  /**
   * Creates a non-nullable version of the given Calcite type.
   *
   * @param calciteType
   *   Original Calcite type
   * @param typeFactory
   *   Type factory for creating the non-nullable type
   * @return
   *   Non-nullable version of the type
   */
  def makeNonNullable(calciteType: RelDataType, typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createTypeWithNullability(calciteType, false)
  }
}
