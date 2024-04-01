/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName}

/**
 * Data type based skipping index column and algorithm selection.
 */
class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  /**
   * Recommend skipping index columns and algorithm.
   *
   * @param tableName
   *   table name
   * @param columns
   *   list of columns
   * @return
   *   skipping index recommendation dataframe
   */
  override def analyzeSkippingIndexColumns(
      tableName: String,
      columns: List[String],
      spark: SparkSession): Seq[Row] = {

    val table = getTable(tableName, spark)
    val partitionFields = getPartitionFields(table)
    val recommendations = new RecommendationRules

    val result = ArrayBuffer[Row]()
    getColumnsList(table, columns).foreach(column => {
      val field = findField(table.schema(), column).get
      if (partitionFields.contains(column)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          recommendations.getSkippingType("PARTITION"),
          recommendations.getReason("PARTITION"))
      } else if (recommendations.containsRule(field.dataType.toString)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          recommendations.getSkippingType(field.dataType.toString),
          recommendations.getReason(field.dataType.toString))
      }
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

  private def getColumnsList(table: Table, columns: List[String]): ListBuffer[String] = {
    val columnsList = ListBuffer[String]()
    if (columns.isEmpty) {
      table.schema().fields.map { field =>
        columnsList += field.name
      }
    } else {
      columnsList.appendAll(columns)
    }
    columnsList
  }

  private def getTable(tableName: String, spark: SparkSession): Table = {
    val (catalog, ident) = parseTableName(spark, tableName)
    loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))
  }
}
