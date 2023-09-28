/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndexOptions.empty

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.flint.{loadTable, parseTableName}

/**
 * Flint Spark index builder base class.
 *
 * @param flint
 *   Flint Spark API entrypoint
 */
abstract class FlintSparkIndexBuilder(flint: FlintSpark) {

  /** Source table name */
  protected var tableName: String = ""

  /** Index options */
  protected var indexOptions: FlintSparkIndexOptions = empty

  /** All columns of the given source table */
  lazy protected val allColumns: Map[String, Column] = {
    require(tableName.nonEmpty, "Source table name is not provided")

    val (catalog, ident) = parseTableName(flint.spark, tableName)
    val table = loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $tableName is not found"))

    // Ref to CatalogImpl.listColumns(): Varchar/Char is StringType with real type name in metadata
    table
      .schema()
      .fields
      .map { field =>
        field.name -> new Column(
          name = field.name,
          description = field.getComment().orNull,
          dataType =
            CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType).catalogString,
          nullable = field.nullable,
          isPartition = false, // useless for now so just set to false
          isBucket = false)
      }
      .toMap
  }

  /**
   * Add index options.
   *
   * @param options
   *   index options
   * @return
   *   builder
   */
  def options(options: FlintSparkIndexOptions): this.type = {
    this.indexOptions = options
    this
  }

  /**
   * Create Flint index.
   */
  def create(): Unit = flint.createIndex(buildIndex())

  /**
   * Build method for concrete builder class to implement
   */
  protected def buildIndex(): FlintSparkIndex

  /**
   * Find column with the given name.
   */
  protected def findColumn(colName: String): Column =
    allColumns.getOrElse(
      colName,
      throw new IllegalArgumentException(s"Column $colName does not exist"))
}
