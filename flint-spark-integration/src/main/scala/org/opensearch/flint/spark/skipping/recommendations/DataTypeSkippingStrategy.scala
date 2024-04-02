/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer

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
   * @param inputs
   *   inputs for recommendation strategy. This can table name, columns or functions.
   * @return
   *   skipping index recommendation dataframe
   */
  override def analyzeSkippingIndexColumns(
      inputs: Map[String, List[String]],
      spark: SparkSession): Seq[Row] = {

    val table = getTable(inputs, spark)
    val partitionFields = getPartitionFields(table)
    val recommendations = new RecommendationRules

    val result = ArrayBuffer[Row]()
    getColumnsList(inputs, spark).foreach(column => {
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

  private def getColumnsList(
      inputs: Map[String, List[String]],
      spark: SparkSession): List[String] = {
    val table = getTable(inputs, spark)
    if (inputs.contains("columns") && (!inputs.get("columns").get.isEmpty)) {
      inputs.get("columns").get
    } else {
      table.schema().fields.map(field => field.name).toList
    }
  }

  private def getTable(inputs: Map[String, List[String]], spark: SparkSession): Table = {
    if (inputs.contains("table name")) {
      val tableName = inputs.get("table name").get(0)
      val (catalog, ident) = parseTableName(spark, tableName)
      loadTable(catalog, ident).getOrElse(
        throw new IllegalStateException(s"Table $tableName is not found"))
    } else {
      throw new IllegalArgumentException("Table name not found")
    }
  }
}
