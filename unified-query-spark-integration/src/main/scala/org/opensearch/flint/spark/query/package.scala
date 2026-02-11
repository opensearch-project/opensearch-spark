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
 */
package object query {

  /**
   * Spark DataType to Calcite RelDataType conversion with nullability support.
   */
  implicit class SparkTypeOps(val sparkType: DataType) extends AnyVal {

    def toCalcite(typeFactory: RelDataTypeFactory): RelDataType = sparkType match {
      case BooleanType => typeFactory.createSqlType(SqlTypeName.BOOLEAN)

      // Numeric types
      case ByteType => typeFactory.createSqlType(SqlTypeName.TINYINT)
      case ShortType => typeFactory.createSqlType(SqlTypeName.SMALLINT)
      case IntegerType => typeFactory.createSqlType(SqlTypeName.INTEGER)
      case LongType => typeFactory.createSqlType(SqlTypeName.BIGINT)
      case FloatType => typeFactory.createSqlType(SqlTypeName.REAL) // 4-byte single-precision
      case DoubleType => typeFactory.createSqlType(SqlTypeName.DOUBLE)
      case dt: DecimalType =>
        typeFactory.createSqlType(SqlTypeName.DECIMAL, dt.precision, dt.scale)

      // String types
      case StringType => typeFactory.createSqlType(SqlTypeName.VARCHAR)
      case vt: VarcharType => typeFactory.createSqlType(SqlTypeName.VARCHAR, vt.length)
      case ct: CharType => typeFactory.createSqlType(SqlTypeName.CHAR, ct.length)

      // Binary type
      case BinaryType => typeFactory.createSqlType(SqlTypeName.VARBINARY)

      // Date/Time types
      case DateType => typeFactory.createSqlType(SqlTypeName.DATE)
      case TimestampType =>
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) // TIMESTAMP_LTZ
      case TimestampNTZType => typeFactory.createSqlType(SqlTypeName.TIMESTAMP)

      // Complex types with nullability
      case ArrayType(elementType, containsNull) =>
        val elementCalciteType = elementType.toCalcite(typeFactory)
        val nullableElement =
          typeFactory.createTypeWithNullability(elementCalciteType, containsNull)
        typeFactory.createArrayType(nullableElement, -1)

      case MapType(keyType, valueType, valueContainsNull) =>
        val keyCalciteType = keyType.toCalcite(typeFactory)
        val valueCalciteType = valueType.toCalcite(typeFactory)
        val nullableValue =
          typeFactory.createTypeWithNullability(valueCalciteType, valueContainsNull)
        typeFactory.createMapType(keyCalciteType, nullableValue)

      case struct: StructType =>
        val fieldNames = struct.fields.map(_.name).toList.asJava
        val fieldTypes = struct.fields
          .map { field =>
            val baseType = field.dataType.toCalcite(typeFactory)
            withNullability(typeFactory, baseType, field.nullable)
          }
          .toList
          .asJava
        typeFactory.createStructType(fieldTypes, fieldNames)

      // Null type
      case NullType => typeFactory.createSqlType(SqlTypeName.NULL)

      // Interval types - map based on (startField, endField)
      case dt: DayTimeIntervalType =>
        import DayTimeIntervalType._
        (dt.startField, dt.endField) match {
          case (DAY, DAY) => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY)
          case (DAY, HOUR) => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_HOUR)
          case (DAY, MINUTE) => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_MINUTE)
          case (DAY, SECOND) => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND)
          case (HOUR, HOUR) => typeFactory.createSqlType(SqlTypeName.INTERVAL_HOUR)
          case (HOUR, MINUTE) => typeFactory.createSqlType(SqlTypeName.INTERVAL_HOUR_MINUTE)
          case (HOUR, SECOND) => typeFactory.createSqlType(SqlTypeName.INTERVAL_HOUR_SECOND)
          case (MINUTE, MINUTE) => typeFactory.createSqlType(SqlTypeName.INTERVAL_MINUTE)
          case (MINUTE, SECOND) => typeFactory.createSqlType(SqlTypeName.INTERVAL_MINUTE_SECOND)
          case (SECOND, SECOND) => typeFactory.createSqlType(SqlTypeName.INTERVAL_SECOND)
          case _ => typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY_SECOND) // fallback
        }

      case ym: YearMonthIntervalType =>
        import YearMonthIntervalType._
        (ym.startField, ym.endField) match {
          case (YEAR, YEAR) => typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR)
          case (YEAR, MONTH) => typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH)
          case (MONTH, MONTH) => typeFactory.createSqlType(SqlTypeName.INTERVAL_MONTH)
          case _ => typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH) // fallback
        }

      // User-defined types - delegate to underlying SQL type
      case udt: UserDefinedType[_] => udt.sqlType.toCalcite(typeFactory)

      // Default fallback for unsupported types (CalendarIntervalType, ObjectType, etc.)
      case _ => typeFactory.createSqlType(SqlTypeName.ANY)
    }
  }

  /**
   * Applies nullability to a Calcite type. Struct types require enforceTypeWithNullability to
   * make the struct itself nullable rather than pushing nullability to fields.
   */
  private def withNullability(
      typeFactory: RelDataTypeFactory,
      calciteType: RelDataType,
      nullable: Boolean): RelDataType = {
    if (calciteType.isStruct) {
      typeFactory.enforceTypeWithNullability(calciteType, nullable)
    } else {
      typeFactory.createTypeWithNullability(calciteType, nullable)
    }
  }
}
