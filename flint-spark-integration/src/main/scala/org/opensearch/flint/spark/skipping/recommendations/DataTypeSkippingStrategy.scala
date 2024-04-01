/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{Row, SparkSession}
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
    val columnsList = ListBuffer[String]()

    if (columns.isEmpty) {
      table.schema().fields.map { field =>
        columnsList += field.name
      }
    } else {
      columnsList.appendAll(columns)
    }

    val recommendations = new RecommendationRules
    columnsList.foreach(column => {
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

}
