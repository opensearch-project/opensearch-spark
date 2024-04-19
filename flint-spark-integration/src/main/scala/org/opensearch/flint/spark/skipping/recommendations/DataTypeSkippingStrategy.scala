/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

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
   *   data for static rule based recommendation.
   * @param spark
   *   spark session
   * @return
   *   skipping index recommendation dataframe
   */
  override def analyzeSkippingIndexColumns(data: DataFrame, spark: SparkSession): Seq[Row] = {
    getTableNameList(data).flatMap(tableName => {
      val table = getTable(tableName, spark)
      val partitionFields = getPartitionFields(table)
      getColumnNameList(data, tableName, spark).flatMap(column => {
        val field = findField(table.schema(), column).get
        val rule = if (partitionFields.contains(column)) {
          "PARTITION"
        } else if (RecommendationRules.containsRule(field.dataType.toString)) {
          field.dataType.toString
        } else {
          null.asInstanceOf[String]
        }
        if (!rule.isEmpty) {
          Some(
            Row(
              field.name,
              field.dataType.typeName,
              RecommendationRules.getSkippingType(rule),
              RecommendationRules.getReason(rule)))
        } else {
          None
        }
      })
    })
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

  private def getColumnNameList(
      data: DataFrame,
      tableName: String,
      spark: SparkSession): List[String] = {
    var columns: List[String] = data
      .filter(s"tableName = '$tableName'")
      .select("columnName")
      .distinct()
      .collect()
      .map(_.getString(0))
      .toList
      .filter(_ != null)

    if (columns.isEmpty) {
      columns = getTable(tableName, spark).schema().fields.map(field => field.name).toList
    }
    columns
  }

  private def getTableNameList(data: DataFrame): List[String] = {
    data
      .select("tableName")
      .distinct
      .collect()
      .map(_.getString(0))
      .toList
  }

  private def getTable(tableName: String, spark: SparkSession): Table = {
    val (catalog, ident) = parseTableName(spark, tableName: String)
    loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))
  }
}
