/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.flint.{loadTable, parseTableName}

class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  override def analyzeSkippingIndexColumns(tableName: String, spark: SparkSession): Seq[Row] = {
    val rules: Config = ConfigFactory.load("skipping_index_recommendation.conf")

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
          rules.getString("recommendation.data_type_rules.PARTITION.skipping_type"),
          rules.getString("recommendation.data_type_rules.PARTITION.reason"))
      } else if (rules.hasPath("recommendation.data_type_rules." + field.dataType.toString)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          rules.getString(
            "recommendation.data_type_rules." + field.dataType.toString + ".skipping_type"),
          rules.getString(
            "recommendation.data_type_rules." + field.dataType.toString + ".reason"))
      }
    }
    result
  }
}
