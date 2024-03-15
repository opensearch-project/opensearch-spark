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
          Row(field.name, field.dataType.toString, getRecommendation("PARTITION")._1, getRecommendation("PARTITION")._2)
        } else {
          Row(field.name, field.dataType.toString, getRecommendation(field.dataType.typeName)._1, getRecommendation(field.dataType.toString)._2)
        }
    }.toSeq
  }

  private def getRecommendation(dataTypeName: String): (String, String) = {
    dataTypeName match {
      case "PARTITION" => (PARTITION.toString, "PARTITION data structure is recommended for partition columns")
      case "BooleanType" => (VALUE_SET.toString, "VALUE_SET data structure is recommended for BooleanType columns")
      case "IntegerType" => (MIN_MAX.toString, "MIN_MAX data structure is recommended for IntegerType columns")
      case "LongType" => (MIN_MAX.toString, "MIN_MAX data structure is recommended for LongType columns")
      case "ShortType" => (MIN_MAX.toString, "MIN_MAX data structure is recommended for ShortType columns")
      case "DateType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for DateType columns")
      case "TimestampType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for TimestampType columns")
      case "StringType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for StringType columns")
      case "VarcharType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for VarcharType columns")
      case "CharType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for CharType columns")
      case "StructType" => (BLOOM_FILTER.toString, "BLOOM_FILTER data structure is recommended for StructType columns")
    }
  }
}
