/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.recommendations

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.flint.{findField, loadTable, parseTableName}
import org.apache.spark.sql.types.{StructField, StructType}

class DataTypeSkippingStrategy extends AnalyzeSkippingStrategy {

  override def analyzeSkippingIndexColumns(
      tableName: String,
      columns: Map[String, Set[String]],
      spark: SparkSession): Seq[Row] = {
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
    val columnsMap = columns

    if (columnsMap.isEmpty) {
      table.schema().fields.map { field =>
        columnsMap += (field.name -> Set.empty)
      }
    }

    columnsMap.foreach(column => {
      val field = findField(table.schema(), column._1).get
      if (partitionFields.contains(column._1)) {
        result += Row(
          field.name,
          field.dataType.typeName,
          rules.getString("recommendation.table_partition.PARTITION.skipping_type"),
          rules.getString("recommendation.table_partition.PARTITION.reason"))
      } else if (!column._2.isEmpty) {
        column._2.foreach(function => {
          result += Row(
            field.name,
            field.dataType.typeName,
            rules.getString("recommendation.function_rules." + function + ".skipping_type"),
            rules.getString("recommendation.function_rules." + function + ".reason"))
        })
      } else {
        result += Row(
          field.name,
          field.dataType.typeName,
          rules.getString(
            "recommendation.data_type_rules." + field.dataType.toString + ".skipping_type"),
          rules.getString(
            "recommendation.data_type_rules." + field.dataType.toString + ".reason"))
      }
    })
    result
  }

  protected def findColumn(allColumns: StructType, colName: String): Column =
    findField(allColumns, colName)
      .map(field => convertFieldToColumn(colName, field))
      .getOrElse(throw new IllegalArgumentException(s"Column $colName does not exist"))

  private def convertFieldToColumn(colName: String, field: StructField): Column = {
    // Ref to CatalogImpl.listColumns(): Varchar/Char is StringType with real type name in metadata
    new Column(
      name = colName,
      description = field.getComment().orNull,
      dataType =
        CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType).catalogString,
      nullable = field.nullable,
      isPartition = false, // useless for now so just set to false
      isBucket = false)
  }
}
