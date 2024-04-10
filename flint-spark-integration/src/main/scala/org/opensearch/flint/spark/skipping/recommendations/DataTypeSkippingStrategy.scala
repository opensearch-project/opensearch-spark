/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName}

/**
 * Data type based skipping index column and algorithm selection.
 */
class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param data
   *   data for static rule based recommendation. This can table name and columns.
   * @return
   *   skipping index recommendation dataframe
   */
  override def analyzeSkippingIndexColumns(data: DataFrame, spark: SparkSession): Seq[Row] = {
    val result = ArrayBuffer[Row]()
    data
      .select("tableName")
      .distinct
      .collect()
      .map(_.getString(0))
      .foreach(tableName => {
        val table = getTable(tableName, spark)
        val partitionFields = getPartitionFields(table)
        var columns: List[String] = data
          .filter(s"tableName = '$tableName'")
          .select("columns")
          .distinct()
          .collect()
          .map(_.getString(0))
          .toList
          .filter(_ != null)

        if (columns.isEmpty) {
          columns = table.schema().fields.map(field => field.name).toList
        }
        columns.foreach(column => {
          val field = findField(table.schema(), column).get
          if (partitionFields.contains(column)) {
            result += Row(
              field.name,
              field.dataType.typeName,
              RecommendationRules.getSkippingType("PARTITION"),
              RecommendationRules.getReason("PARTITION"))
          } else if (RecommendationRules.containsRule(field.dataType.toString)) {
            result += Row(
              field.name,
              field.dataType.typeName,
              RecommendationRules.getSkippingType(field.dataType.toString),
              RecommendationRules.getReason(field.dataType.toString))
          }
        })
      })
    result
  }

  private def getPartitionFields(table: Table): Array[String] = {
    table.partitioning().flatMap { transform =>
      transform
        .references()
        .collect({ case reference =>
          reference.fieldNames()
        })
        .flatten
        .toSet
    }
  }

  private def getTable(tableName: String, spark: SparkSession): Table = {
    val (catalog, ident) = parseTableName(spark, tableName: String)
    loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))
  }
}
