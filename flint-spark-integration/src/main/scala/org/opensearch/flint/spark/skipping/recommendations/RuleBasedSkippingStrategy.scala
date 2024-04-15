/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName}
import org.apache.spark.sql.functions.col

/**
 * Data type based skipping index column and algorithm selection.
 */
class RuleBasedSkippingStrategy extends AnalyzeSkippingStrategy {

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
        var columns: Set[String] = data
          .filter(s"tableName = '$tableName'")
          .select("column")
          .distinct()
          .collect()
          .map(_.getString(0))
          .toSet
          .filter(_ != null)

        if (columns.isEmpty) {
          columns = table.schema().fields.map(field => field.name).toSet
        }
        columns.foreach(column => {
          val field = findField(table.schema(), column).get
          val functions: Set[String] = data
            .filter(col("tableName") === tableName && col("column") === column)
            .select("function")
            .distinct()
            .collect()
            .map(_.getString(0))
            .toSet
            .filter(_ != null)

          if (!functions.isEmpty) {
            functions.foreach(function => {
              result += Row(
                field.name,
                field.dataType.typeName,
                RecommendationRules.getSkippingType(function),
                RecommendationRules.getReason(function))
            })
          } else if (partitionFields.contains(column)) {
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
    val (catalog, ident) = parseTableName(spark, tableName)
    loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))
  }
}
