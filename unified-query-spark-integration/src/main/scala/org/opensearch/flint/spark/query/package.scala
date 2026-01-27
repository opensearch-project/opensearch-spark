/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql.`type`.SqlTypeName

import org.apache.spark.sql.types._

/**
 * Type conversions between Spark and Calcite type systems.
 *
 * Usage: {{{ import org.opensearch.flint.spark.query._ val calciteType =
 * sparkDataType.toCalcite(typeFactory) }}}
 */
package object query {

  /**
   * Spark DataType to Calcite RelDataType conversion.
   */
  implicit class SparkTypeOps(val sparkType: DataType) extends AnyVal {

    def toCalcite(typeFactory: RelDataTypeFactory): RelDataType = sparkType match {
      case BooleanType => typeFactory.createSqlType(SqlTypeName.BOOLEAN)

      // Numeric types
      case ByteType => typeFactory.createSqlType(SqlTypeName.TINYINT)
      case ShortType => typeFactory.createSqlType(SqlTypeName.SMALLINT)
      case IntegerType => typeFactory.createSqlType(SqlTypeName.INTEGER)
      case LongType => typeFactory.createSqlType(SqlTypeName.BIGINT)
      case FloatType => typeFactory.createSqlType(SqlTypeName.FLOAT)
      case DoubleType => typeFactory.createSqlType(SqlTypeName.DOUBLE)
      case dt: DecimalType =>
        typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)

      // String types
      case StringType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case _: VarcharType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case _: CharType => typeFactory.createSqlType(SqlTypeName.CHAR)

      // Binary type
      case BinaryType => typeFactory.createSqlType(SqlTypeName.VARBINARY)

      // Date/Time types
      case DateType => typeFactory.createSqlType(SqlTypeName.DATE)
      case TimestampType => typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
      case TimestampNTZType => typeFactory.createSqlType(SqlTypeName.TIMESTAMP)

      // Complex types
      case ArrayType(elementType, _) =>
        typeFactory.createArrayType(elementType.toCalcite(typeFactory), -1)

      case MapType(keyType, valueType, _) =>
        typeFactory.createMapType(
          keyType.toCalcite(typeFactory),
          valueType.toCalcite(typeFactory))

      case struct: StructType =>
        val fieldNames = struct.fields.map(_.name).toList.asJava
        val fieldTypes = struct.fields.map(_.dataType.toCalcite(typeFactory)).toList.asJava
        typeFactory.createStructType(fieldTypes, fieldNames)

      // Null type
      case NullType => typeFactory.createSqlType(SqlTypeName.NULL)

      // Interval types
      case CalendarIntervalType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case _: DayTimeIntervalType => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND)
      case _: YearMonthIntervalType => typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH)

      // Default fallback
      case _ => typeFactory.createSqlType(SqlTypeName.ANY)
    }
  }
}
