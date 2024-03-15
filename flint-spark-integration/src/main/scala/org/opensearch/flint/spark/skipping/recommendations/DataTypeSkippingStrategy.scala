/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, MIN_MAX, PARTITION, VALUE_SET}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.flint.{loadTable, parseTableName}

class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  override def analyzeSkippingIndexColumns(tableName: String, spark: SparkSession): Seq[Row] = {
    require(tableName.nonEmpty, "Source table name is not provided")

    val (catalog, ident) = parseTableName(spark, tableName)
    val table = loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))

    val partitionFields = table.partitioning().flatMap {
      transform =>
        transform.references().collect({
          case reference => reference.fieldNames()
        }).flatten.toSet
    }

    table.schema().fields.map {
      field =>
        if (partitionFields.contains(field.name)) {
          Row(field.name, field.dataType.toString, PARTITION.toString, getRule(PARTITION.toString))
        } else {
          val reason = getRule(field.dataType.toString)
          field.dataType.toString match {
            case "BooleanType" =>
              Row(field.name, field.dataType.typeName, VALUE_SET.toString, reason)
            case "IntegerType" | "LongType" | "ShortType" =>
              Row(field.name, field.dataType.typeName, MIN_MAX.toString, reason)
            case "DateType" | "TimestampType" | "StringType" | "VarcharType" | "CharType" | "StructType" =>
              Row(field.name, field.dataType.typeName, BLOOM_FILTER.toString, reason)
          }
        }
    }.toSeq
  }

  private def getRule(dataTypeName: String): String = {
    dataTypeName match {
      case "PARTITION" => "PARTITION data structure is recommended for partition columns"
      case "BooleanType" => "VALUE_SET data structure is recommended for BooleanType columns"
      case "IntegerType" => "MIN_MAX data structure is recommended for IntegerType columns"
      case "LongType" => "MIN_MAX data structure is recommended for LongType columns"
      case "ShortType" => "MIN_MAX data structure is recommended for ShortType columns"
      case "DateType" => "MIN_MAX data structure is recommended for DateType columns"
      case "TimestampType" => "MIN_MAX data structure is recommended for TimestampType columns"
      case "StringType" => "MIN_MAX data structure is recommended for StringType columns"
      case "VarcharType" => "MIN_MAX data structure is recommended for VarcharType columns"
      case "CharType" => "MIN_MAX data structure is recommended for CharType columns"
      case "StructType" => "MIN_MAX data structure is recommended for StructType columns"
    }
  }

}
