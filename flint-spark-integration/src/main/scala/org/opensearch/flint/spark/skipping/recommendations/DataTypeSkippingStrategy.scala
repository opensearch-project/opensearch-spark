/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKind.{BLOOM_FILTER, MIN_MAX, PARTITION, VALUE_SET}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.flint.{loadTable, parseTableName}

class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  val rules = Map(
    "PARTITION" -> (PARTITION.toString, "PARTITION data structure is recommended for partition columns"),
    "BooleanType" -> (VALUE_SET.toString, "VALUE_SET data structure is recommended for BooleanType columns"),
    "IntegerType" -> (MIN_MAX.toString, "MIN_MAX data structure is recommended for IntegerType columns"),
    "LongType" -> (MIN_MAX.toString, "MIN_MAX data structure is recommended for LongType columns"),
    "ShortType" -> (MIN_MAX.toString, "MIN_MAX data structure is recommended for ShortType columns"),
    "DateType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for DateType columns"),
    "TimestampType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for TimestampType columns"),
    "StringType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for StringType columns"),
    "VarcharType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for VarcharType columns"),
    "CharType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for CharType columns"),
    "StructType" -> (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for StructType columns"))

  override def analyzeSkippingIndexColumns(tableName: String, spark: SparkSession): Seq[Row] = {
    val (catalog, ident) = parseTableName(spark, tableName)
    val table = loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))

    val partitionFields = table.partitioning().flatMap { transform =>
      transform
        .references()
        .collect({ case reference =>
          reference.fieldNames()
        })
        .flatten
        .toSet
    }

    val result = ArrayBuffer[Row]()
    table.schema().fields.map { field =>
      if (partitionFields.contains(field.name)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          rules("PARTITION")._1,
          rules("PARTITION")._2)
      } else if (rules.contains(field.dataType.toString)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          rules(field.dataType.toString)._1,
          rules(field.dataType.toString)._2)
      }
    }
    result
  }
}
