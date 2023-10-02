/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.opensearch.flint.spark.FlintSparkIndexOptions.empty

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.flint.{loadTable, parseTableName, qualifyTableName}
import org.apache.spark.sql.types.StructField

/**
 * Flint Spark index builder base class.
 *
 * @param flint
 *   Flint Spark API entrypoint
 */
abstract class FlintSparkIndexBuilder(flint: FlintSpark) {

  /** Qualified source table name */
  protected var qualifiedTableName: String = ""

  /** Index options */
  protected var indexOptions: FlintSparkIndexOptions = empty

  /** All columns of the given source table */
  lazy protected val allColumns: Map[String, Column] = {
    require(qualifiedTableName.nonEmpty, "Source table name is not provided")

    val (catalog, ident) = parseTableName(flint.spark, qualifiedTableName)
    val table = loadTable(catalog, ident).getOrElse(
      throw new IllegalStateException(s"Table $qualifiedTableName is not found"))

    val allFields = table.schema().fields
    allFields.map { field => field.name -> convertFieldToColumn(field) }.toMap
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
   *
   * @param ignoreIfExists
   *   ignore existing index
   */
  def create(ignoreIfExists: Boolean = false): Unit =
    flint.createIndex(buildIndex(), ignoreIfExists)

  /**
   * Build method for concrete builder class to implement
   */
  protected def buildIndex(): FlintSparkIndex

  /**
   * Table name setter that qualifies given table name for subclass automatically.
   */
  protected def tableName_=(tableName: String): Unit = {
    qualifiedTableName = qualifyTableName(flint.spark, tableName)
  }

  /**
   * Table name getter
   */
  protected def tableName: String = {
    qualifiedTableName
  }

  /**
   * Find column with the given name.
   */
  protected def findColumn(colName: String): Column =
    allColumns.getOrElse(
      colName,
      throw new IllegalArgumentException(s"Column $colName does not exist"))

  private def convertFieldToColumn(field: StructField): Column = {
    // Ref to CatalogImpl.listColumns(): Varchar/Char is StringType with real type name in metadata
    new Column(
      name = field.name,
      description = field.getComment().orNull,
      dataType =
        CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType).catalogString,
      nullable = field.nullable,
      isPartition = false, // useless for now so just set to false
      isBucket = false)
  }
}
